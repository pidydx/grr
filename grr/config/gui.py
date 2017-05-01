#!/usr/bin/env python
"""Configuration parameters for the admin UI."""

from grr.lib import config_lib

# The Admin UI web application.
config_lib.DEFINE_integer("AdminUI.port", 8000, "port to listen on")

config_lib.DEFINE_integer("AdminUI.port_max", None,
                          "If set and AdminUI.port is in use, attempt to "
                          "use ports between AdminUI.port and "
                          "AdminUI.port_max.")

# Override this if you want to access admin ui extenally. Make sure it is
# secured (i.e. AdminUI.webauth_manager is not NullWebAuthManager)!
config_lib.DEFINE_string("AdminUI.bind", "127.0.0.1", "interface to bind to.")

config_lib.DEFINE_string("AdminUI.document_root", "%(grr/gui/static|resource)",
                         "The main path to the static HTML pages.")

config_lib.DEFINE_string("AdminUI.template_root",
                         "%(grr/gui/templates|resource)",
                         "The main path to the templates.")

config_lib.DEFINE_string("AdminUI.local_document_root",
                         "%(grr/gui/local/static|resource)",
                         "The main path to the localized static HTML pages.")

config_lib.DEFINE_string("AdminUI.help_root", "%(docs|resource)",
                         "The main path to the locally cached documentation.")

config_lib.DEFINE_string(
    "AdminUI.webauth_manager", "NullWebAuthManager",
    "The web auth manager for controlling access to the UI.")

config_lib.DEFINE_string("AdminUI.firebase_api_key", None,
                         "Firebase API key. Used by FirebaseWebAuthManager.")
config_lib.DEFINE_string("AdminUI.firebase_auth_domain", None,
                         "Firebase API key. Used by FirebaseWebAuthManager.")
config_lib.DEFINE_string(
    "AdminUI.firebase_auth_provider", "GoogleAuthProvider",
    "Firebase auth provider (see "
    "https://firebase.google.com/docs/auth/web/start). Used by "
    "FirebaseWebAuthManager.")

# TODO(user): Deprecated, remove at some point.
config_lib.DEFINE_string("AdminUI.django_secret_key", "CHANGE_ME",
                         "This is deprecated. Used csrf_secret_key instead!.")

config_lib.DEFINE_string(
    "AdminUI.csrf_secret_key", "CHANGE_ME",
    "This is a secret key that should be set in the server "
    "config. It is used in CSRF protection.")

config_lib.DEFINE_bool(
    "AdminUI.csrf_cookie_secure", False,
    "Whether to use a secure cookie for the CSRF cookie. If this is set"
    "to True, the cookie will be marked as secure, which means browsers"
    "may ensure that the cookie is only sent with an HTTPS connection.")

config_lib.DEFINE_string(
    "AdminUI.csrf_cookie_domain", None,
    "The domain to be used when setting the CSRF cookie. This can be useful"
    "for easily allowing cross-subdomain requests to be excluded from the"
    "normal cross site request forgery protection. It should be set to a"
    "string such as .example.com to allow a POST request from a form on"
    "one subdomain to be accepted by a view served from another subdomain.")

config_lib.DEFINE_bool("AdminUI.enable_ssl", False,
                       "Turn on SSL. This needs AdminUI.ssl_cert to be set.")

config_lib.DEFINE_string("AdminUI.ssl_cert_file", "",
                         "The SSL certificate to use.")

config_lib.DEFINE_string(
    "AdminUI.ssl_key_file", None,
    "The SSL key to use. The key may also be part of the cert file, in which "
    "case this can be omitted.")

config_lib.DEFINE_string("AdminUI.url", "http://localhost:8000/",
                         "The direct external URL for the user interface.")

config_lib.DEFINE_bool("AdminUI.use_precompiled_js", False,
                       "If True - use Closure-compiled JS bundle. This flag "
                       "is experimental and is not properly supported yet.")

config_lib.DEFINE_string("AdminUI.export_command", "/usr/bin/grr_api_shell "
                         "'%(AdminUI.url)'",
                         "Command to show in the fileview for downloading the "
                         "files from the command line.")

config_lib.DEFINE_string("AdminUI.heading", "",
                         "Dashboard heading displayed in the Admin UI.")

config_lib.DEFINE_string("AdminUI.report_url",
                         "https://github.com/google/grr/issues",
                         "URL of the 'Report a problem' link.")

config_lib.DEFINE_string("AdminUI.help_url", "/help/index.html",
                         "URL of the 'Help' link.")

config_lib.DEFINE_string("AdminUI.github_docs_location",
                         "https://github.com/google/grr-doc/blob/master",
                         "Base path for GitHub-hosted GRR documentation. ")

config_lib.DEFINE_string("AdminUI.new_hunt_wizard.default_output_plugin", None,
                         "Output plugin that will be added by default in the "
                         "'New Hunt' wizard output plugins selection page.")

# Temporary option that allows limiting access to legacy UI renderers. Useful
# when giving access to GRR AdminUI to parties that have to use the HTTP API
# only.
# TODO(user): remove as soon as legacy rendering system is removed.
config_lib.DEFINE_list(
    "AdminUI.legacy_renderers_allowed_groups", [],
    "Users belonging to these  groups can access legacy GRR renderers, "
    "which are still used for some GRR features (manage binaries, legacy "
    "browse virtual filesystem pane, etc). If this option is not set, then "
    "no additional checks are performed when legacy renderers are used.")

config_lib.DEFINE_string(
    "AdminUI.debug_impersonate_user", None,
    "NOTE: for debugging purposes only! If set, every request AdminUI gets "
    "will be attributed to the specified user. Useful for checking how AdminUI "
    "looks like for an access-restricted user.")
