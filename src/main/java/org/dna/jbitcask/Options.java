package org.dna.jbitcask;

import java.util.Properties;
import java.util.function.Function;

public final class Options {

    static class Option<T> {
        private String name;
        private Class<T> type;
        private T defaultValue;
        private Function<String, T> decoder;

        public Option(String optName, Class<T> type) {
            this(optName, type, null);
        }

        public Option(String optName, Class<T> type, T defaultValue) {
            this(optName, type, defaultValue, null);
        }

        public Option(String optName, Class<T> type, T defaultValue, Function<String, T> decoder) {
            this.name = optName;
            this.type = type;
            this.defaultValue = defaultValue;
            this.decoder = decoder;
        }

        public boolean hasDefaultValue() {
            return defaultValue != null;
        }

        public T defaultValue() {
            return defaultValue;
        }

        public T decodeFromString(String strValue) {
            if (decoder != null) {
                return decoder.apply(strValue);
            }
            return null;
        }
    }

    public static final Option<Boolean> READ_WRITE = new Option<>("read_write", Boolean.class, false, Boolean::getBoolean);
    public static final Option<Boolean> SYNC_STRATEGY = new Option<>("sync_strategy", Boolean.class, false, Boolean::getBoolean);
    public static final Option<Integer> EXPIRY_SECS = new Option<>("expiry_secs", Integer.class, 0, Integer::parseInt);
    public static final Option<Long> EXPIRY_TIME = new Option<>("expiry_time", Long.class, 0L, Long::parseLong);
    public static final Option<Long> EXPIRY_GRACE_TIME = new Option<>("expiry_grace_time", Long.class, 0L, Long::parseLong);
    public static final Option<Long> MAX_FILE_SIZE = new Option<>("max_file_size", Long.class, 0L, Long::parseLong);
    public static final Option<Long> OPEN_TIMEOUT = new Option<>("open_timeout", Long.class, 0L, Long::parseLong);
    public static final Option<Byte> TOMBSTONE_VERSION = new Option<>("tombstone_version", Byte.class, (byte)0, Byte::parseByte);

    private final Properties props;

    Options(Properties props) {
        this.props = props;
    }

    public <T> T get(Option<T> option) {
        final Object value = this.props.get(option.name);
        if (value == null)
            if (option.hasDefaultValue()) {
                return option.defaultValue();
            } else {
                return option.decodeFromString(System.getenv(option.name));
            }
        return option.type.cast(value);
    }

    public <T> void add(Option<T> option, T value) {
        this.props.put(option.name, value);
    }
}
