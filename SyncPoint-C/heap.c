#include "heap.h"

void swap(node *nd1, node *nd2){
    node temp = *nd1;
    *nd1 = *nd2;
    *nd2 = temp;
}

void heapify(heap *hp, int i){
    int largest = (LCHILD(i) < hp->size && hp->node_compare(
        hp->elem[LCHILD(i)], hp->elem[i])) ? LCHILD(i) : i;
    if(RCHILD(i) < hp->size && hp->node_compare(hp->elem[RCHILD(i)], hp->elem[largest])){
        largest = RCHILD(i);
    }
    if(largest != i){
        swap(&(hp->elem[i]), &(hp->elem[largest]));
        heapify(hp, largest);
    }
}

void *heap_init(int(*node_compare)(node, node)){
    heap *hp;
    hp = malloc(sizeof(*hp));
    hp->size = 0;
    hp->node_compare = node_compare;
    return hp;
}

void heap_free(heap *hp, void(*node_free)(node)){
    if(hp->size){
        for(int i = 0; i < hp->size; i++){
            node_free(hp->elem[i]);
        }
        free(hp->elem);
    }
    free(hp);
}

void heap_insert_node(heap *hp, node nd){
    // allocating space
    if(hp->size){
        hp->elem = realloc(hp->elem, (hp->size + 1) * sizeof(node));
    } else {
        hp->elem = malloc(sizeof(node));
    }

    // Positioning the node at the right position in the max heap
    int i = (hp->size)++;
    while(i && hp->node_compare(nd, hp->elem[PARENT(i)])){
        hp->elem[i] = hp->elem[PARENT(i)];
        i = PARENT(i);
    }
    hp->elem[i] = nd;
}

void heap_delete_node(heap *hp, int i){
    if(hp->size){
        hp->elem[i] = hp->elem[--(hp->size)];
        hp->elem = realloc(hp->elem, hp->size * sizeof(node));
        heapify(hp, i);
    } else {
        free(hp->elem);
    }
}

void heap_build_heap(heap *hp, node *arr, int size){
    int i;

    // Insertion into the heap without violating the shape property
    for(i = 0; i < size; i++){
        if(hp->size){
            hp->elem = realloc(hp->elem, (hp->size + 1) * sizeof(node));
        } else {
            hp->elem = malloc(sizeof(node));
        }
        hp->elem[(hp->size)++] = arr[i];
    }

    // Making sure that heap property is also satisfied
    for(i = PARENT(hp->size); i >= 0; i--){
        heapify(hp, i);
    }
}
