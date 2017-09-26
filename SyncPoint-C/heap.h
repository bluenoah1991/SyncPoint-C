#ifndef HEAP_DEFINE_
#define HEAP_DEFINE_

#include <stdlib.h>

#define LCHILD(x) 2 * x + 1
#define RCHILD(x) 2 * x + 2
#define PARENT(x) (x - 1) / 2

typedef void * node;

typedef struct heap{
    int size;
    node *elem;
    int (*node_compare)(node node1, node node2);
} heap;

void *heap_init(int(*node_compare)(node, node));

void heap_free(heap *hp, void(*node_free)(node));

void heap_insert_node(heap *hp, node nd);

void heap_delete_node(heap *hp, int i);

void heap_build_heap(heap *hp, node *arr, int size);

#endif