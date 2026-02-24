/*
 * elastic_hash_table.c — Elastic Hash Table implementation
 */

#include "elastic_hash_table.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ------------------------------------------------------------------ */
/* Slot / SubArray definitions                                        */
/* ------------------------------------------------------------------ */

typedef enum { SLOT_EMPTY, SLOT_OCCUPIED, SLOT_TOMBSTONE } SlotState;

typedef struct {
    char*      key;         /* heap-allocated copy, NUL-terminated */
    void*      value;       /* heap-allocated copy                 */
    size_t     value_len;
    SlotState  state;
} Slot;

typedef struct {
    int     level;
    size_t  capacity;
    size_t  count;          /* live entries   */
    size_t  tombstones;
    Slot*   slots;
} SubArray;

struct ElasticHashTable {
    size_t    total_capacity;
    size_t    count;              /* total live entries across all levels */
    size_t    num_levels;
    size_t    min_level_size;
    double    max_load;
    double    tombstone_ratio;
    SubArray* levels;
};

struct EHTIterator {
    ElasticHashTable* table;
    size_t level_idx;
    size_t slot_idx;
};

/* ------------------------------------------------------------------ */
/* FNV-1a (64-bit) with salt — stable, deterministic hash             */
/* ------------------------------------------------------------------ */

static uint64_t fnv1a_salted(const char* key, uint64_t salt)
{
    uint64_t h = UINT64_C(0xcbf29ce484222325) ^ salt;
    for (const unsigned char* p = (const unsigned char*)key; *p; ++p) {
        h ^= (uint64_t)*p;
        h *= UINT64_C(0x100000001b3);
    }
    return h;
}

static void dual_hash(const char* key, int level,
                       uint64_t* h1_out, uint64_t* h2_out)
{
    uint64_t salt1 = (uint64_t)level * UINT64_C(0x9E3779B97F4A7C15) + 0xA1;
    uint64_t salt2 = (uint64_t)level * UINT64_C(0x517CC1B727220A95) + 0xB2;
    *h1_out = fnv1a_salted(key, salt1);
    *h2_out = fnv1a_salted(key, salt2) | 1;  /* odd → full period */
}

static size_t probe_idx(uint64_t h1, uint64_t h2,
                         size_t attempt, size_t capacity)
{
    return (size_t)((h1 + attempt * h2) % capacity);
}

/* ------------------------------------------------------------------ */
/* Probe-budget: O(log²(1/ε))                                        */
/* ------------------------------------------------------------------ */

static size_t probe_budget(const SubArray* sub)
{
    double used = (double)(sub->count + sub->tombstones);
    double eps  = 1.0 - used / (double)sub->capacity;
    if (eps <= 0.0) return sub->capacity;

    double inv_eps = 1.0 / eps;
    double l       = log(inv_eps);
    double budget  = 3.0 + 3.0 * l * l;
    size_t b       = (size_t)budget + 1;
    return b < sub->capacity ? b : sub->capacity;
}

/* ------------------------------------------------------------------ */
/* SubArray helpers                                                    */
/* ------------------------------------------------------------------ */

static int subarray_init(SubArray* sa, int level, size_t capacity)
{
    sa->level     = level;
    sa->capacity  = capacity;
    sa->count     = 0;
    sa->tombstones = 0;
    sa->slots     = (Slot*)calloc(capacity, sizeof(Slot));
    if (!sa->slots) return -1;
    /* calloc zeroes everything; SLOT_EMPTY == 0 */
    return 0;
}

static void slot_free_data(Slot* s)
{
    free(s->key);
    free(s->value);
    s->key       = NULL;
    s->value     = NULL;
    s->value_len = 0;
}

static void subarray_destroy(SubArray* sa)
{
    if (!sa->slots) return;
    for (size_t i = 0; i < sa->capacity; ++i) {
        if (sa->slots[i].state == SLOT_OCCUPIED)
            slot_free_data(&sa->slots[i]);
    }
    free(sa->slots);
    sa->slots = NULL;
}

/* ------------------------------------------------------------------ */
/* Level construction                                                 */
/* ------------------------------------------------------------------ */

static int build_levels(ElasticHashTable* t, size_t capacity)
{
    /* Count how many levels we need */
    size_t remaining = capacity;
    size_t n_levels  = 0;
    size_t tmp       = remaining;
    while (tmp > t->min_level_size * 2) {
        tmp -= tmp / 2;
        ++n_levels;
    }
    ++n_levels; /* final remainder level */

    t->levels = (SubArray*)malloc(n_levels * sizeof(SubArray));
    if (!t->levels) return -1;
    t->num_levels = n_levels;

    remaining = capacity;
    for (size_t i = 0; i < n_levels - 1; ++i) {
        size_t sz = remaining / 2;
        if (subarray_init(&t->levels[i], (int)i, sz) < 0) return -1;
        remaining -= sz;
    }
    /* Last level gets the remainder */
    if (subarray_init(&t->levels[n_levels - 1],
                       (int)(n_levels - 1), remaining) < 0)
        return -1;

    return 0;
}

/* ------------------------------------------------------------------ */
/* Create / Destroy                                                   */
/* ------------------------------------------------------------------ */

ElasticHashTable* eht_create(size_t total_capacity)
{
    if (total_capacity < 64) total_capacity = 64;

    ElasticHashTable* t = (ElasticHashTable*)calloc(1, sizeof(*t));
    if (!t) return NULL;

    t->total_capacity  = total_capacity;
    t->count           = 0;
    t->min_level_size  = 16;
    t->max_load        = 0.90;
    t->tombstone_ratio = 0.15;

    if (build_levels(t, total_capacity) < 0) {
        free(t);
        return NULL;
    }
    return t;
}

void eht_destroy(ElasticHashTable* t)
{
    if (!t) return;
    for (size_t i = 0; i < t->num_levels; ++i)
        subarray_destroy(&t->levels[i]);
    free(t->levels);
    free(t);
}

/* ------------------------------------------------------------------ */
/* Internal: find a key → (level_idx, slot_idx) or (-1, 0)            */
/* ------------------------------------------------------------------ */

typedef struct { int level_idx; size_t slot_idx; } FindResult;

static FindResult find_key(ElasticHashTable* t, const char* key)
{
    FindResult r = { -1, 0 };
    for (size_t li = 0; li < t->num_levels; ++li) {
        SubArray* sub = &t->levels[li];
        if (sub->count == 0) continue;

        size_t budget = probe_budget(sub);
        uint64_t h1, h2;
        dual_hash(key, sub->level, &h1, &h2);

        for (size_t a = 0; a < budget; ++a) {
            size_t idx = probe_idx(h1, h2, a, sub->capacity);
            Slot*  s   = &sub->slots[idx];

            if (s->state == SLOT_OCCUPIED && strcmp(s->key, key) == 0) {
                r.level_idx = (int)li;
                r.slot_idx  = idx;
                return r;
            }
            if (s->state == SLOT_EMPTY)
                break;  /* not at this level; try next */
        }
    }
    return r;
}

/* ------------------------------------------------------------------ */
/* Internal: insert taking ownership of key/value pointers            */
/* ------------------------------------------------------------------ */

static int insert_owned(ElasticHashTable* t,
                         char* key, void* value, size_t value_len);

/* Forward-declared rebuild */
static int rebuild(ElasticHashTable* t, size_t new_capacity);

static int insert_owned(ElasticHashTable* t,
                         char* key, void* value, size_t value_len)
{
    for (size_t li = 0; li < t->num_levels; ++li) {
        SubArray* sub = &t->levels[li];
        size_t budget = probe_budget(sub);
        uint64_t h1, h2;
        dual_hash(key, sub->level, &h1, &h2);

        for (size_t a = 0; a < budget; ++a) {
            size_t idx = probe_idx(h1, h2, a, sub->capacity);
            Slot*  s   = &sub->slots[idx];

            if (s->state == SLOT_EMPTY || s->state == SLOT_TOMBSTONE) {
                if (s->state == SLOT_TOMBSTONE)
                    sub->tombstones--;

                s->key       = key;
                s->value     = value;
                s->value_len = value_len;
                s->state     = SLOT_OCCUPIED;
                sub->count++;
                t->count++;
                return 0;
            }
        }
    }
    /* All levels exhausted — grow and retry */
    if (rebuild(t, t->total_capacity * 2) < 0) return -1;
    return insert_owned(t, key, value, value_len);
}

/* ------------------------------------------------------------------ */
/* Rebuild / resize                                                   */
/* ------------------------------------------------------------------ */

static int rebuild(ElasticHashTable* t, size_t new_capacity)
{
    /* 1. Collect live entries (steal pointers) */
    size_t      old_count  = t->count;
    char**      keys       = (char**)malloc(old_count * sizeof(char*));
    void**      vals       = (void**)malloc(old_count * sizeof(void*));
    size_t*     lens       = (size_t*)malloc(old_count * sizeof(size_t));
    if (!keys || !vals || !lens) {
        free(keys); free(vals); free(lens);
        return -1;
    }

    size_t ci = 0;
    for (size_t li = 0; li < t->num_levels; ++li) {
        SubArray* sub = &t->levels[li];
        for (size_t si = 0; si < sub->capacity; ++si) {
            Slot* s = &sub->slots[si];
            if (s->state == SLOT_OCCUPIED) {
                keys[ci] = s->key;
                vals[ci] = s->value;
                lens[ci] = s->value_len;
                s->key   = NULL;    /* prevent double-free */
                s->value = NULL;
                ++ci;
            }
        }
    }

    /* 2. Destroy old levels */
    for (size_t i = 0; i < t->num_levels; ++i)
        subarray_destroy(&t->levels[i]);
    free(t->levels);
    t->levels     = NULL;
    t->num_levels = 0;
    t->count      = 0;

    /* 3. Build new levels */
    t->total_capacity = new_capacity;
    if (build_levels(t, new_capacity) < 0) {
        /* catastrophic — free collected entries */
        for (size_t i = 0; i < ci; ++i) { free(keys[i]); free(vals[i]); }
        free(keys); free(vals); free(lens);
        return -1;
    }

    /* 4. Re-insert (ownership transfer — no copies) */
    for (size_t i = 0; i < ci; ++i)
        insert_owned(t, keys[i], vals[i], lens[i]);

    free(keys);
    free(vals);
    free(lens);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Public: insert                                                     */
/* ------------------------------------------------------------------ */

int eht_insert(ElasticHashTable* t,
               const char* key,
               const void* value, size_t value_len)
{
    /* Update-in-place if already present */
    FindResult fr = find_key(t, key);
    if (fr.level_idx >= 0) {
        Slot* s = &t->levels[fr.level_idx].slots[fr.slot_idx];
        void* new_val = malloc(value_len);
        if (!new_val && value_len > 0) return -1;
        memcpy(new_val, value, value_len);
        free(s->value);
        s->value     = new_val;
        s->value_len = value_len;
        return 0;
    }

    /* Check load / tombstones → rebuild if needed */
    if (t->count >= (size_t)(t->total_capacity * t->max_load))
        if (rebuild(t, t->total_capacity * 2) < 0) return -1;

    {
        size_t total_ts = 0;
        for (size_t i = 0; i < t->num_levels; ++i)
            total_ts += t->levels[i].tombstones;
        if (total_ts >= (size_t)(t->total_capacity * t->tombstone_ratio))
            if (rebuild(t, t->total_capacity) < 0) return -1;
    }

    /* Copy key and value, then insert with ownership transfer */
    size_t klen = strlen(key) + 1;
    char*  kdup = (char*)malloc(klen);
    void*  vdup = malloc(value_len);
    if (!kdup || (!vdup && value_len > 0)) {
        free(kdup); free(vdup);
        return -1;
    }
    memcpy(kdup, key, klen);
    memcpy(vdup, value, value_len);

    return insert_owned(t, kdup, vdup, value_len);
}

/* ------------------------------------------------------------------ */
/* Public: get                                                        */
/* ------------------------------------------------------------------ */

int eht_get(ElasticHashTable* t,
            const char* key,
            const void** value_out, size_t* len_out)
{
    FindResult fr = find_key(t, key);
    if (fr.level_idx < 0) return 0;

    Slot* s   = &t->levels[fr.level_idx].slots[fr.slot_idx];
    *value_out = s->value;
    *len_out   = s->value_len;
    return 1;
}

/* ------------------------------------------------------------------ */
/* Public: delete                                                     */
/* ------------------------------------------------------------------ */

int eht_delete(ElasticHashTable* t, const char* key)
{
    FindResult fr = find_key(t, key);
    if (fr.level_idx < 0) return 0;

    SubArray* sub = &t->levels[fr.level_idx];
    Slot*     s   = &sub->slots[fr.slot_idx];
    slot_free_data(s);
    s->state = SLOT_TOMBSTONE;
    sub->count--;
    sub->tombstones++;
    t->count--;
    return 1;
}

/* ------------------------------------------------------------------ */
/* Public: contains                                                   */
/* ------------------------------------------------------------------ */

int eht_contains(ElasticHashTable* t, const char* key)
{
    return find_key(t, key).level_idx >= 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* Public: metadata                                                   */
/* ------------------------------------------------------------------ */

size_t eht_len(const ElasticHashTable* t)        { return t->count; }
size_t eht_capacity(const ElasticHashTable* t)    { return t->total_capacity; }
size_t eht_num_levels(const ElasticHashTable* t)  { return t->num_levels; }

void eht_level_stats(const ElasticHashTable* t,
                      EHTLevelInfo* out, size_t max_levels)
{
    size_t n = t->num_levels < max_levels ? t->num_levels : max_levels;
    for (size_t i = 0; i < n; ++i) {
        out[i].level      = t->levels[i].level;
        out[i].capacity   = t->levels[i].capacity;
        out[i].count      = t->levels[i].count;
        out[i].tombstones = t->levels[i].tombstones;
    }
}

/* ------------------------------------------------------------------ */
/* Public: iteration                                                  */
/* ------------------------------------------------------------------ */

EHTIterator* eht_iter_create(ElasticHashTable* t)
{
    EHTIterator* it = (EHTIterator*)calloc(1, sizeof(*it));
    if (!it) return NULL;
    it->table     = t;
    it->level_idx = 0;
    it->slot_idx  = 0;
    return it;
}

int eht_iter_next(EHTIterator* it,
                  const char** key_out,
                  const void** value_out,
                  size_t* len_out)
{
    ElasticHashTable* t = it->table;
    while (it->level_idx < t->num_levels) {
        SubArray* sub = &t->levels[it->level_idx];
        while (it->slot_idx < sub->capacity) {
            Slot* s = &sub->slots[it->slot_idx];
            it->slot_idx++;
            if (s->state == SLOT_OCCUPIED) {
                *key_out   = s->key;
                *value_out = s->value;
                *len_out   = s->value_len;
                return 1;
            }
        }
        it->level_idx++;
        it->slot_idx = 0;
    }
    return 0;
}

void eht_iter_destroy(EHTIterator* it)
{
    free(it);
}
