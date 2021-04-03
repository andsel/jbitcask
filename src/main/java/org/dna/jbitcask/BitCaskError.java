package org.dna.jbitcask;

import javax.management.RuntimeErrorException;

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

class MergeLockedError extends BitCaskError {}

class NotReadyError extends BitCaskError {}

class KeyTxError extends BitCaskError {}

class NotFoundError extends BitCaskError {}

class ExpiredError extends BitCaskError {}

class UndefinedError extends BitCaskError {}

class NoTombstoneError extends BitCaskError {}

class InvariantViolationError extends RuntimeErrorException {

    public InvariantViolationError(Error e) {
        super(e);
    }

    public InvariantViolationError(Error e, String message) {
        super(e, message);
    }

    public InvariantViolationError(byte[] key, byte[] value, long oldFileId, long oldOffset, long outFileId, long offset) {
        this(null, String.format("%s %d %d -> %d %d", new String(key), oldFileId, oldOffset, outFileId, offset));
    }
}

class IterationInProcessError extends BitCaskError {}

class OutOfDateError extends BitCaskError {}

class IterationNotStartedError extends BitCaskError {}

