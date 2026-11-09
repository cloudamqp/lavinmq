/* Fails on a stock bdwgc 8.2.12 with parallel marking: the global mark stack
 * overflows while the roots are pushed (Crystal pushes every parked fiber's
 * stack with GC_push_all_eager) and never grows, so every collection falls
 * back to a serial heap rescan. With mark-stack-overflow.patch it grows once
 * GC_set_grow_mark_stack(1) is called, and only then.
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

/* Collects a few times, exits if any object is lost, and returns the */
/* mark stack size */
static size_t collect(void)
{
  size_t i;
  for (i = 0; i < 4; i++)
    GC_gcollect();
  for (i = 0; i < N_ROOTS; i++)
    if (*(size_t *)roots[i] != i) {
      printf("object %zu clobbered\n", i);
      exit(1);
    }
  return GC_mark_stack_size;
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
  if (collect() != initial) {
    printf("mark stack grew to %zu entries while switched off\n",
           GC_mark_stack_size);
    return 1;
  }
  GC_set_grow_mark_stack(1);
  if (!GC_get_grow_mark_stack() || collect() <= initial) {
    printf("mark stack did not grow\n");
    return 1;
  }
  printf("mark stack %zu -> %zu entries, %d marker threads\n", initial,
         GC_mark_stack_size, GC_get_parallel());
  return 0;
}
