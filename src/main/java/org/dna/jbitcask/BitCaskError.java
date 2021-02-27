package org.dna.jbitcask;

public class BitCaskError extends RuntimeException {
    public BitCaskError(String s) {
        super(s);
    }

    public BitCaskError(Throwable ex) {
        super(ex);
    }
}
