#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>

#include "internal.hpp"

namespace xgc2 {
namespace adapter_runtime {
namespace internal {

namespace {

constexpr std::array<std::uint32_t, 64> kRoundConstants = {{
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U,
    0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
    0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U, 0xe49b69c1U, 0xefbe4786U,
    0x0fc19dc6U, 0x240ca1ccU, 0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
    0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
    0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
    0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U, 0xa2bfe8a1U, 0xa81a664bU,
    0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
    0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU,
    0x5b9cca4fU, 0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
    0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
}};

std::uint32_t RotateRight(std::uint32_t value, unsigned shift) {
  return (value >> shift) | (value << (32U - shift));
}

class Sha256 final {
 public:
  Sha256()
      : state_{{0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU, 0x510e527fU,
                0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U}} {}

  void Update(const void* input, std::size_t length) {
    const auto* bytes = static_cast<const std::uint8_t*>(input);
    total_bytes_ += static_cast<std::uint64_t>(length);
    while (length != 0) {
      const std::size_t copy = std::min(length, block_.size() - buffered_bytes_);
      std::memcpy(block_.data() + buffered_bytes_, bytes, copy);
      buffered_bytes_ += copy;
      bytes += copy;
      length -= copy;
      if (buffered_bytes_ == block_.size()) {
        Transform(block_.data());
        buffered_bytes_ = 0;
      }
    }
  }

  std::array<std::uint8_t, 32> Finish() {
    const std::uint64_t bit_length = total_bytes_ * 8U;
    block_[buffered_bytes_++] = 0x80U;
    if (buffered_bytes_ > 56) {
      std::fill(block_.begin() + buffered_bytes_, block_.end(), 0U);
      Transform(block_.data());
      buffered_bytes_ = 0;
    }
    std::fill(block_.begin() + buffered_bytes_, block_.begin() + 56, 0U);
    for (std::size_t index = 0; index < 8; ++index) {
      block_[63 - index] = static_cast<std::uint8_t>(bit_length >> (index * 8U));
    }
    Transform(block_.data());

    std::array<std::uint8_t, 32> result{};
    for (std::size_t word = 0; word < state_.size(); ++word) {
      for (std::size_t byte = 0; byte < 4; ++byte) {
        result[word * 4 + byte] =
            static_cast<std::uint8_t>(state_[word] >> ((3U - byte) * 8U));
      }
    }
    return result;
  }

 private:
  void Transform(const std::uint8_t* block) {
    std::array<std::uint32_t, 64> words{};
    for (std::size_t index = 0; index < 16; ++index) {
      words[index] = (static_cast<std::uint32_t>(block[index * 4]) << 24U) |
                     (static_cast<std::uint32_t>(block[index * 4 + 1]) << 16U) |
                     (static_cast<std::uint32_t>(block[index * 4 + 2]) << 8U) |
                     static_cast<std::uint32_t>(block[index * 4 + 3]);
    }
    for (std::size_t index = 16; index < words.size(); ++index) {
      const std::uint32_t s0 = RotateRight(words[index - 15], 7) ^
                               RotateRight(words[index - 15], 18) ^
                               (words[index - 15] >> 3U);
      const std::uint32_t s1 = RotateRight(words[index - 2], 17) ^
                               RotateRight(words[index - 2], 19) ^
                               (words[index - 2] >> 10U);
      words[index] = words[index - 16] + s0 + words[index - 7] + s1;
    }

    std::uint32_t a = state_[0];
    std::uint32_t b = state_[1];
    std::uint32_t c = state_[2];
    std::uint32_t d = state_[3];
    std::uint32_t e = state_[4];
    std::uint32_t f = state_[5];
    std::uint32_t g = state_[6];
    std::uint32_t h = state_[7];
    for (std::size_t index = 0; index < words.size(); ++index) {
      const std::uint32_t upper_sigma1 =
          RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
      const std::uint32_t choice = (e & f) ^ ((~e) & g);
      const std::uint32_t temporary1 =
          h + upper_sigma1 + choice + kRoundConstants[index] + words[index];
      const std::uint32_t upper_sigma0 =
          RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
      const std::uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
      const std::uint32_t temporary2 = upper_sigma0 + majority;
      h = g;
      g = f;
      f = e;
      e = d + temporary1;
      d = c;
      c = b;
      b = a;
      a = temporary1 + temporary2;
    }
    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
  }

  std::array<std::uint32_t, 8> state_;
  std::array<std::uint8_t, 64> block_{};
  std::size_t buffered_bytes_ = 0;
  std::uint64_t total_bytes_ = 0;
};

}  // namespace

bool TerminalDigest(const google::protobuf::Message& message, std::string* digest,
                    std::string* error) {
  if (digest == nullptr || message.GetDescriptor() == nullptr) {
    SetError(error, "terminal protobuf message and digest output are required");
    return false;
  }
  std::string encoded;
  {
    google::protobuf::io::StringOutputStream output(&encoded);
    google::protobuf::io::CodedOutputStream coded(&output);
    coded.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
      SetError(error, "terminal protobuf message could not be encoded");
      return false;
    }
  }

  Sha256 hash;
  const std::string& type_name = message.GetDescriptor()->full_name();
  hash.Update(type_name.data(), type_name.size());
  const char separator = '\0';
  hash.Update(&separator, 1);
  hash.Update(encoded.data(), encoded.size());
  const auto bytes = hash.Finish();

  std::ostringstream formatted;
  formatted << "sha256:" << std::hex << std::setfill('0');
  for (const std::uint8_t byte : bytes) {
    formatted << std::setw(2) << static_cast<unsigned>(byte);
  }
  *digest = formatted.str();
  return true;
}

}  // namespace internal
}  // namespace adapter_runtime
}  // namespace xgc2
