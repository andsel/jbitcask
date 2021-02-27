package org.dna.jbitcask;

final class FunctionResult<A, R> {
    enum Atom {OK, ERROR, READY, NOT_READY}

    private final A atom;

    private final R result;

    public FunctionResult(A atom, R result) {
        this.atom = atom;
        this.result = result;
    }

    public A getAtom() {
        return atom;
    }

    public R getResult() {
        return result;
    }
}
