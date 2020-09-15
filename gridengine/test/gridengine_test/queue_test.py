from gridengine.queue import tokenize_pe_list


def test_pe_list_tokenizing() -> None:
    assert [
        "NONE",
        "@mpihosts=mpi mpislots",
        "@smphosts=smp1,smp2",
    ] == tokenize_pe_list("NONE,[@mpihosts=mpi mpislots],[@smphosts=smp1,smp2")
