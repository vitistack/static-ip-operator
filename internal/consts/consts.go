package consts

var (
	DEVELOPMENT             = "DEVELOPMENT"
	LOG_LEVEL               = "LOG_LEVEL"
	LOG_JSON                = "LOG_JSON"
	LOG_ADD_CALLER          = "LOG_ADD_CALLER"
	LOG_DISABLE_STACKTRACE  = "LOG_DISABLE_STACKTRACE"
	LOG_UNESCAPED_MULTILINE = "LOG_UNESCAPED_MULTILINE"
	LOG_COLORIZE_LINE       = "LOG_COLORIZE_LINE"

	// STATIC_IP_STRICT_DEFAULTS, when "true", makes the operator refuse to
	// fall back to listing NetworkNamespaces when spec.networkNamespaceName
	// is unset on a NetworkConfiguration. When unset or false (default), the
	// operator falls back to listing for backward compatibility and logs a
	// deprecation notice once per resource.
	STATIC_IP_STRICT_DEFAULTS = "STATIC_IP_STRICT_DEFAULTS"
)
