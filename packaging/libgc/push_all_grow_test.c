/* GC_push_all_grow (push-all-grow.patch) pushes more ranges than the
 * initial mark stack holds without aborting or overflowing, the way Crystal
 * pushes every parked fiber's stack, and every object stays reachable.
 * Built by the Makefile inside the bdwgc source tree, for the private header. */
#include <stdio.h>
#include <stdlib.h>
#include "private/gc_priv.h"
#include "gc_mark.h"

#define N_ROOTS 20000

static void **roots;
static GC_push_other_roots_proc prev;

static void push_roots(void)
{
  size_t i;
  if (prev) prev();
  for (i = 0; i < N_ROOTS; i++)
    GC_push_all_grow(&roots[i], &roots[i] + 1);
}

int main(void)
{
  size_t initial, i;
  GC_set_markers_count(2); /* parallel marking even on a single CPU */
  GC_INIT();
  GC_allow_register_threads(); /* starts the marker threads */
  initial = GC_mark_stack_size;
  if (initial >= N_ROOTS) {
    printf("mark stack already holds %zu entries, nothing to test\n", initial);
    return 1;
  }
  roots = calloc(N_ROOTS, sizeof *roots);
  prev = GC_get_push_other_roots();
  GC_set_push_other_roots(push_roots);
  for (i = 0; i < N_ROOTS; i++) {
    size_t *o = GC_MALLOC(sizeof *o);
    *o = i;
    roots[i] = o;
  }
  for (i = 0; i < 4; i++) {
    GC_gcollect();
    /* Reuse memory freed by a collection that missed a root */
    GC_MALLOC(N_ROOTS * sizeof(size_t));
  }
  for (i = 0; i < N_ROOTS; i++)
    if (*(size_t *)roots[i] != i) {
      printf("object %zu clobbered\n", i);
      return 1;
    }
  printf("push_all_grow: mark stack %zu -> %zu entries, %d marker threads\n",
         initial, GC_mark_stack_size, GC_get_parallel());
  if (GC_mark_stack_size < N_ROOTS) {
    printf("mark stack did not grow\n");
    return 1;
  }
  return 0;
}
