package org.dna.jbitcask;

public class BitCaskError extends Error {
    public BitCaskError() {}

    public BitCaskError(String s) {
        super(s);
    }

    public BitCaskError(Throwable ex) {
        super(ex);
    }
}

class AlreadyExistsError extends BitCaskError {
    public AlreadyExistsError() {
        super();
    }

    public AlreadyExistsError(String s) {
        super(s);
    }

    public AlreadyExistsError(Throwable ex) {
        super(ex);
    }
}

class KeyNotFoundError extends BitCaskError {}

class ReadOnlyError extends BitCaskError {}

class NoFileError extends BitCaskError {}

class BadCrcError extends BitCaskError {}