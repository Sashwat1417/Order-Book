#pragma once

#include <string>

// Java analogy: a utility class (final, no instances) for SMTP via libcurl.
// Think JavaMailSender but using Gmail SMTP over TLS (port 465).
namespace EmailHelper {

    struct SmtpConfig {
        std::string gmailUser;
        std::string appPassword;
        std::string recipient;
    };

    // Sends a plain-text email via Gmail SMTPS (port 465).
    // Throws std::runtime_error on failure.
    void sendEmail(const SmtpConfig& cfg,
                   const std::string& subject,
                   const std::string& body);

}  // namespace EmailHelper
