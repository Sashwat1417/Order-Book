#include "EmailHelper.hpp"

#include <curl/curl.h>
#include <stdexcept>
#include <cstring>

namespace EmailHelper {

// libcurl calls this to read the email payload chunk by chunk.
// Java analogy: InputStream.read() backing the MimeMessage body.
struct UploadCtx {
    const std::string* payload;
    size_t offset = 0;
};

static size_t readCallback(char* ptr, size_t size, size_t nmemb, void* userp) {
    auto* ctx       = static_cast<UploadCtx*>(userp);
    size_t available = ctx->payload->size() - ctx->offset;
    size_t toCopy    = std::min(size * nmemb, available);
    if (toCopy == 0) return 0;
    std::memcpy(ptr, ctx->payload->c_str() + ctx->offset, toCopy);
    ctx->offset += toCopy;
    return toCopy;
}

void sendEmail(const SmtpConfig& cfg,
               const std::string& subject,
               const std::string& body)
{
    // RFC 2822 email payload
    std::string payload =
        "To: "      + cfg.recipient  + "\r\n"
        "From: "    + cfg.gmailUser  + "\r\n"
        "Subject: " + subject        + "\r\n"
        "\r\n"      + body           + "\r\n";

    UploadCtx ctx{&payload, 0};

    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("curl_easy_init() failed");

    curl_slist* rcptList = nullptr;
    rcptList = curl_slist_append(rcptList, cfg.recipient.c_str());

    std::string mailFrom = "<" + cfg.gmailUser + ">";

    curl_easy_setopt(curl, CURLOPT_URL,           "smtps://smtp.gmail.com:465");
    curl_easy_setopt(curl, CURLOPT_USERNAME,      cfg.gmailUser.c_str());
    curl_easy_setopt(curl, CURLOPT_PASSWORD,      cfg.appPassword.c_str());
    curl_easy_setopt(curl, CURLOPT_MAIL_FROM,     mailFrom.c_str());
    curl_easy_setopt(curl, CURLOPT_MAIL_RCPT,     rcptList);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION,  readCallback);
    curl_easy_setopt(curl, CURLOPT_READDATA,      &ctx);
    curl_easy_setopt(curl, CURLOPT_UPLOAD,        1L);
    curl_easy_setopt(curl, CURLOPT_USE_SSL,       CURLUSESSL_ALL);

    CURLcode res = curl_easy_perform(curl);
    curl_slist_free_all(rcptList);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        throw std::runtime_error(
            std::string("Email send failed: ") + curl_easy_strerror(res));
    }
}

}  // namespace EmailHelper
