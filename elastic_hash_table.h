/*
 * elastic_hash_table.h — Elastic Hash Table (Farach-Colton et al., 2025)
 *
 * A C implementation of an open-addressing hash table whose address space
 * is split into geometrically decreasing sub-arrays.  Insertions cascade
 * from the largest (densest) level to smaller (sparser) levels, yielding
 * excellent cache behaviour and O(1) expected operations.
 *
 * Keys:   NUL-terminated C strings  (copied internally)
 * Values: Opaque byte buffers       (copied internally)
 */

#ifndef ELASTIC_HASH_TABLE_H
#define ELASTIC_HASH_TABLE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle */
typedef struct ElasticHashTable ElasticHashTable;

/* Per-level diagnostic info */
typedef struct {
    int      level;
    size_t   capacity;
    size_t   count;       /* live entries          */
    size_t   tombstones;
} EHTLevelInfo;

/* Opaque iterator */
typedef struct EHTIterator EHTIterator;

/* ---------- Lifecycle ---------- */

ElasticHashTable* eht_create(size_t total_capacity);
void              eht_destroy(ElasticHashTable* t);

/* ---------- Core operations ---------- */

/*  Returns 0 on success, -1 on allocation failure. */
int  eht_insert(ElasticHashTable* t,
                const char* key,
                const void* value, size_t value_len);

/*  Returns 1 if found (and sets *value_out, *len_out), 0 if not found.
 *  The returned pointer is into internal storage — copy it before mutating
 *  the table. */
int  eht_get(ElasticHashTable* t,
             const char* key,
             const void** value_out, size_t* len_out);

/*  Returns 1 if key was present and deleted, 0 if not found. */
int  eht_delete(ElasticHashTable* t, const char* key);

/*  Returns 1 if key is present, 0 otherwise. */
int  eht_contains(ElasticHashTable* t, const char* key);

/* ---------- Metadata ---------- */

size_t eht_len(const ElasticHashTable* t);
size_t eht_capacity(const ElasticHashTable* t);
size_t eht_num_levels(const ElasticHashTable* t);
void   eht_level_stats(const ElasticHashTable* t,
                        EHTLevelInfo* out, size_t max_levels);

/* ---------- Iteration ---------- */

EHTIterator* eht_iter_create(ElasticHashTable* t);
/*  Advances the iterator.  Returns 1 and populates the out-params if
 *  there is a next entry, 0 at end-of-table. */
int          eht_iter_next(EHTIterator* it,
                           const char** key_out,
                           const void** value_out,
                           size_t* len_out);
void         eht_iter_destroy(EHTIterator* it);

#ifdef __cplusplus
}
#endif

#endif /* ELASTIC_HASH_TABLE_H */
