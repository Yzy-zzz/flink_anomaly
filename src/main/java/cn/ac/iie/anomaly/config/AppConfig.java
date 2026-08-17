package cn.ac.iie.anomaly.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public final class AppConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Properties properties;

    private AppConfig(Properties properties) {
        this.properties = properties;
    }

    public static AppConfig load(String[] args) throws IOException {
        String explicit = findConfigArg(args);
        if (explicit == null || explicit.trim().isEmpty()) {
            explicit = System.getProperty("anomaly.config");
        }

        Properties props = new Properties();
        if (explicit != null && !explicit.trim().isEmpty()) {
            loadFile(props, new File(explicit.trim()), true);
        } else {
            File local = new File("application.properties");
            if (local.isFile()) {
                loadFile(props, local, true);
            } else {
                try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
                    if (in == null) {
                        throw new IOException("application.properties not found in current directory or classpath");
                    }
                    props.load(in);
                }
            }
        }
        applySystemPropertyOverrides(props);
        return new AppConfig(props);
    }

    private static void loadFile(Properties props, File file, boolean required) throws IOException {
        if (!file.isFile()) {
            if (required) {
                throw new IOException("Config file not found: " + file.getAbsolutePath());
            }
            return;
        }
        try (InputStream in = new FileInputStream(file)) {
            props.load(in);
        }
    }

    private static String findConfigArg(String[] args) {
        if (args == null) {
            return null;
        }
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (args[i] != null && args[i].startsWith("--config=")) {
                return args[i].substring("--config=".length());
            }
        }
        return null;
    }

    private static void applySystemPropertyOverrides(Properties props) {
        for (String name : props.stringPropertyNames()) {
            String override = System.getProperty(name);
            if (override != null) {
                props.setProperty(name, override);
            }
        }
    }

    public String get(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return value.trim();
    }

    public String get(String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : value.trim();
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    public long getLong(String key, long defaultValue) {
        return Long.parseLong(get(key, String.valueOf(defaultValue)));
    }

    public double getDouble(String key, double defaultValue) {
        return Double.parseDouble(get(key, String.valueOf(defaultValue)));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }

    public Properties copyProperties() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }
}
