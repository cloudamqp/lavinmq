---------------------------------- MODULE MC ----------------------------------
(* Definitions for the models (MC_*.cfg), which TLC config files can't express. *)
EXTENDS MessagesOnRaft

\* Every message goes to the same segment, so deleting it needs all acked
MCFileOf == [m \in Msg |-> CHOOSE f \in File : TRUE]

MCSymmetry == Permutations(Server)
===============================================================================
