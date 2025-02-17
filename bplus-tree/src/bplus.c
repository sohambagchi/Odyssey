#include <stdlib.h> /* malloc */
#include <string.h> /* strlen */

#include "bplus.h"
#include "private/utils.h"

int bp_open(bp_db_t *tree, const char* filename)
{
    int ret;

    ret = pthread_rwlock_init(&tree->rwlock, NULL) ? BP_ERWLOCK : BP_OK;
    if (ret != BP_OK) return ret;

    ret = bp__writer_create((bp__writer_t*) tree, filename);
    if (ret != BP_OK) goto fatal;

    tree->head.page = NULL;

    ret = bp__init(tree);
    if (ret != BP_OK) goto fatal;

    return BP_OK;

fatal:
    pthread_rwlock_destroy(&tree->rwlock);
    return ret;
}

int bp_close(bp_db_t *tree)
{
    pthread_rwlock_wrlock(&tree->rwlock);
    bp__destroy(tree);
    pthread_rwlock_unlock(&tree->rwlock);

    pthread_rwlock_destroy(&tree->rwlock);
    return BP_OK;
}

int bp__init(bp_db_t *tree)
{
    int ret;
    /*
     * Load head.
     * Writer will not compress data chunk smaller than head,
     * that's why we're passing head size as compressed size here
     */
    ret = bp__writer_find((bp__writer_t *) tree,
                          kNotCompressed,
                          BP__HEAD_SIZE,
                          &tree->head,
                          bp__tree_read_head,
                          bp__tree_write_head);
    if (ret == BP_OK) {
        /* set default compare function */
        bp_set_compare_cb(tree, bp__default_compare_cb);
    }

    return ret;
}

void bp__destroy(bp_db_t *tree)
{
    bp__writer_destroy((bp__writer_t *) tree);
    if (tree->head.page != NULL) {
        bp__page_destroy(tree, tree->head.page);
        tree->head.page = NULL;
    }
}

int bp_get(bp_db_t *tree, const bp_key_t* key, bp_value_t *value)
{
    int ret;

    pthread_rwlock_rdlock(&tree->rwlock);

    ret = bp__page_get(tree, tree->head.page, key, value);

    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}


int bp_get_previous(bp_db_t *tree,
                    const bp_value_t *value,
                    bp_value_t *previous)
{
    if (value->_prev_offset == 0 && value->_prev_length == 0) {
        return BP_ENOTFOUND;
    }
    return bp__value_load(tree,
                          value->_prev_offset,
                          value->_prev_length,
                          previous);
}

int bp_update(bp_db_t *tree,
              const bp_key_t *key,
              const bp_value_t *value,
              bp_update_cb update_cb,
              void *arg)
{
    int ret;
    printf("Before lock\n");
    pthread_rwlock_wrlock(&tree->rwlock);
    printf("Is the code reaching here then?\n");
    ret = bp__page_insert(tree, tree->head.page, key, value, update_cb, arg);
    if (ret == BP_OK) {
        ret = bp__tree_write_head((bp__writer_t*) tree, NULL);
    }

    pthread_rwlock_unlock(&tree->rwlock);
    printf("After lcok\n");
    return ret;
}

int bp_bulk_update(bp_db_t *tree,
                   const uint64_t count,
                   const bp_key_t **keys,
                   const bp_value_t **values,
                   bp_update_cb update_cb,
                   void *arg)
{
    int ret;
    bp_key_t *keys_iter = (bp_key_t *) *keys;
    bp_value_t* values_iter = (bp_value_t *) *values;
    uint64_t left = count;

    pthread_rwlock_wrlock(&tree->rwlock);

    ret = bp__page_bulk_insert(tree,
                               tree->head.page,
                               NULL,
                               &left,
                               &keys_iter,
                               &values_iter,
                               update_cb,
                               arg);
    if (ret == BP_OK) {
        ret =  bp__tree_write_head((bp__writer_t *) tree, NULL);
    }

    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}


int bp_set(bp_db_t *tree, const bp_key_t *key, const bp_value_t *value)
{
    return bp_update(tree, key, value, NULL, NULL);
}


int bp_bulk_set(bp_db_t *tree,
                const uint64_t count,
                const bp_key_t **keys,
                const bp_value_t **values)
{
    return bp_bulk_update(tree, count, keys, values, NULL, NULL);
}


int bp_removev(bp_db_t *tree,
               const bp_key_t *key,
               bp_remove_cb remove_cb,
               void *arg)
{
    int ret;

    pthread_rwlock_wrlock(&tree->rwlock);

    ret = bp__page_remove(tree, tree->head.page, key, remove_cb, arg);
    if (ret == BP_OK) {
        ret = bp__tree_write_head((bp__writer_t *) tree, NULL);
    }

    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}

int bp_remove(bp_db_t *tree, const bp_key_t *key)
{
    return bp_removev(tree, key, NULL, NULL);
}

int bp_compact(bp_db_t *tree)
{
    int ret;
    char *compacted_name;
    bp_db_t compacted;

    /* get name of compacted database (prefixed with .compact) */
    ret = bp__writer_compact_name((bp__writer_t *) tree, &compacted_name);
    if (ret != BP_OK) return ret;

    /* open it */
    ret = bp_open(&compacted, compacted_name);
    free(compacted_name);
    if (ret != BP_OK) return ret;

    /* destroy stub head page */
    bp__page_destroy(&compacted, compacted.head.page);

    pthread_rwlock_rdlock(&tree->rwlock);

    /* clone source tree's head page */
    ret = bp__page_clone(&compacted, tree->head.page, &compacted.head.page);

    pthread_rwlock_unlock(&tree->rwlock);

    /* copy all pages starting from head */
    ret = bp__page_copy(tree, &compacted, compacted.head.page);
    if (ret != BP_OK) return ret;

    ret = bp__tree_write_head((bp__writer_t *) &compacted, NULL);
    if (ret != BP_OK) return ret;

    pthread_rwlock_wrlock(&tree->rwlock);

    ret = bp__writer_compact_finalize((bp__writer_t *) tree,
                                      (bp__writer_t *) &compacted);
    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}

int bp_get_filtered_range(bp_db_t *tree,
                          const bp_key_t *start,
                          const bp_key_t *end,
                          bp_filter_cb filter,
                          bp_range_cb cb,
                          void *arg)
{
    int ret;

    pthread_rwlock_rdlock(&tree->rwlock);

    ret = bp__page_get_range(tree,
                             tree->head.page,
                             start,
                             end,
                             filter,
                             cb,
                             arg);

    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}

int bp_get_range(bp_db_t *tree,
                 const bp_key_t *start,
                 const bp_key_t *end,
                 bp_range_cb cb,
                 void *arg)
{
    return bp_get_filtered_range(tree,
                                 start,
                                 end,
                                 bp__default_filter_cb,
                                 cb,
                                 arg);
}

/* Wrappers to allow string to string set/get/remove */

int bp_gets(bp_db_t *tree, const char *key, char **value)
{
    int ret;
    bp_key_t bkey;
    bp_value_t bvalue;

    BP__STOVAL(key, bkey);

    ret = bp_get(tree, &bkey, &bvalue);
    if (ret != BP_OK) return ret;

    *value = bvalue.value;

    return BP_OK;
}

int bp_updates(bp_db_t *tree,
               const char *key,
               const char *value,
               bp_update_cb update_cb,
               void *arg)
{
    bp_key_t bkey;
    bp_value_t bvalue;

    BP__STOVAL(key, bkey);
    BP__STOVAL(value, bvalue);

    printf("Is the code reaching here?\n");
    return bp_update(tree, &bkey, &bvalue, update_cb, arg);
}


int bp_sets(bp_db_t *tree, const char *key, const char *value)
{
    printf("Tree is: %p\n", tree);
    return bp_updates(tree, key, value, NULL, NULL);
}

int bp_bulk_updates(bp_db_t *tree,
                    const uint64_t count,
                    const char **keys,
                    const char **values,
                    bp_update_cb update_cb,
                    void *arg)
{
    int ret;
    bp_key_t *bkeys;
    bp_value_t *bvalues;
    uint64_t i;

    /* allocated memory for keys/values */
    bkeys = malloc(sizeof(*bkeys) * count);
    if (bkeys == NULL) return BP_EALLOC;

    bvalues = malloc(sizeof(*bvalues) * count);
    if (bvalues == NULL) {
        free(bkeys);
        return BP_EALLOC;
    }

    /* copy keys/values to allocated memory */
    for (i = 0; i < count; i++) {
        BP__STOVAL(keys[i], bkeys[i]);
        BP__STOVAL(values[i], bvalues[i]);
    }

    ret = bp_bulk_update(tree,
                         count,
                         (const bp_key_t **) &bkeys,
                         (const bp_value_t **) &bvalues,
                         update_cb,
                         arg);

    free(bkeys);
    free(bvalues);

    return ret;
}

int bp_bulk_sets(bp_db_t *tree,
                 const uint64_t count,
                 const char **keys,
                 const char **values)
{
    return bp_bulk_updates(tree, count, keys, values, NULL, NULL);
}

int bp_removevs(bp_db_t *tree,
                const char *key,
                bp_remove_cb remove_cb,
                void *arg)
{
    bp_key_t bkey;

    BP__STOVAL(key, bkey);

    return bp_removev(tree, &bkey, remove_cb, arg);
}

int bp_removes(bp_db_t *tree, const char *key)
{
    return bp_removevs(tree, key, NULL, NULL);
}

int bp_get_filtered_ranges(bp_db_t *tree,
                           const char *start,
                           const char *end,
                           bp_filter_cb filter,
                           bp_range_cb cb,
                           void *arg)
{
    bp_key_t bstart;
    bp_key_t bend;

    BP__STOVAL(start, bstart);
    BP__STOVAL(end, bend);

    return bp_get_filtered_range(tree, &bstart, &bend, filter, cb, arg);
}

int bp_get_ranges(bp_db_t *tree,
                  const char *start,
                  const char *end,
                  bp_range_cb cb,
                  void *arg)
{
    return bp_get_filtered_ranges(tree,
                                  start,
                                  end,
                                  bp__default_filter_cb,
                                  cb,
                                  arg);
}

/* various functions */

void bp_set_compare_cb(bp_db_t *tree, bp_compare_cb cb)
{
    tree->compare_cb = cb;
}


int bp_fsync(bp_db_t *tree)
{
    int ret;

    pthread_rwlock_wrlock(&tree->rwlock);
    ret = bp__writer_fsync((bp__writer_t *) tree);
    pthread_rwlock_unlock(&tree->rwlock);

    return ret;
}

/* internal utils */

int bp__tree_read_head(bp__writer_t *w, void *data)
{
    int ret;
    bp_db_t *t = (bp_db_t *) w;
    bp__tree_head_t* head = (bp__tree_head_t *) data;

    t->head.offset = ntohll(head->offset);
    t->head.config = ntohll(head->config);
    t->head.page_size = ntohll(head->page_size);
    t->head.hash = ntohll(head->hash);

    /* we've copied all data - free it */
    free(data);

    /* Check hash first */
    if (bp__compute_hashl(t->head.offset) != t->head.hash) return 1;

    ret = bp__page_load(t, t->head.offset, t->head.config, &t->head.page);
    if (ret != BP_OK) return ret;

    t->head.page->is_head = 1;

    return ret;
}

int bp__tree_write_head(bp__writer_t *w, void *data)
{
    int ret;
    bp_db_t* t = (bp_db_t*) w;
    bp__tree_head_t nhead;
    uint64_t offset;
    uint64_t size;

    if (t->head.page == NULL) {
        /* TODO: page size should be configurable */
        t->head.page_size = 64;

        /* Create empty leaf page */
        ret = bp__page_create(t, kLeaf, 0, 1, &t->head.page);
        if (ret != BP_OK) return ret;

        t->head.page->is_head = 1;
    }

    /* Update head's position */
    t->head.offset = t->head.page->offset;
    t->head.config = t->head.page->config;

    t->head.hash = bp__compute_hashl(t->head.offset);

    /* Create temporary head with fields in network byte order */
    nhead.offset = htonll(t->head.offset);
    nhead.config = htonll(t->head.config);
    nhead.page_size = htonll(t->head.page_size);
    nhead.hash = htonll(t->head.hash);

    size = BP__HEAD_SIZE;
    ret = bp__writer_write(w,
                           kNotCompressed,
                           &nhead,
                           &offset,
                           &size);

    return ret;
}

int bp__default_compare_cb(const bp_key_t *a, const bp_key_t *b)
{
    uint32_t len = a->length < b->length ? a->length : b->length;

    for (uint32_t i = 0; i < len; i++) {
        if (a->value[i] != b->value[i])
            return (uint8_t) a->value[i] > (uint8_t) b->value[i] ? 1 : -1;
    }

    return a->length - b->length;
}


int bp__default_filter_cb(void *arg, const bp_key_t *key)
{
    /* default filter accepts all keys */
    return 1;
}
