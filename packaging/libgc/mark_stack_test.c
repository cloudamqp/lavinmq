/* Fails on a stock bdwgc 8.2.12 with parallel marking: the global mark stack
 * overflows while the roots are pushed (Crystal pushes every parked fiber's
 * stack with GC_push_all_eager) and never grows, so every collection falls
 * back to a serial heap rescan. With mark-stack-overflow.patch it grows.
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
  if (prev) prev();
  GC_push_all_eager(roots, roots + N_ROOTS);
}

int main(void)
{
  size_t initial, i;
  GC_set_markers_count(2); /* parallel marking even on a single CPU */
  GC_INIT();
  GC_allow_register_threads(); /* starts the marker threads */
  if (!GC_get_parallel()) {
    printf("no marker threads (GC_MARKERS set?), nothing to test\n");
    return 1;
  }
  initial = GC_mark_stack_size;
  roots = calloc(N_ROOTS, sizeof *roots);
  prev = GC_get_push_other_roots();
  GC_set_push_other_roots(push_roots);
  for (i = 0; i < N_ROOTS; i++) {
    size_t *o = GC_MALLOC(sizeof *o);
    *o = i;
    roots[i] = o;
  }
  for (i = 0; i < 4; i++)
    GC_gcollect();
  for (i = 0; i < N_ROOTS; i++)
    if (*(size_t *)roots[i] != i) {
      printf("object %zu clobbered\n", i);
      return 1;
    }
  printf("mark stack %zu -> %zu entries, %d marker threads\n", initial,
         GC_mark_stack_size, GC_get_parallel());
  if (GC_mark_stack_size <= initial) {
    printf("mark stack did not grow\n");
    return 1;
  }
  return 0;
}
