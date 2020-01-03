#include "common/temporary_file.hpp"
#include <cerrno>

namespace mesos {
namespace internal {

/*
 * Represent a temporary file that can be either written or read from.
 */
TemporaryFile::TemporaryFile() {
  Try<string> filepath = os::mktemp();
  if(filepath.isError()){
    throw std::runtime_error(
      "Unable to create temporary file to run commands : " + std::strerror(errno));
  }
  m_filepath = filepath.get()
}

/*
 * Read whole content of the temporary file.
 * @return The content of the file.
 */
std::string TemporaryFile::readAll() const {
  std::ifstream ifs;
  ifs.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  try {
    ifs.open(m_filepath);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));
    ifs.close();
  }
  catch(std::ifstream::failure &e) {
    throw std::runtime_error("Exception while accessing temporary file : " + std::strerror(errno));
  }
  return content;
}

/*
 * Write content to the temporary file.
 * @param content The content to write to the file.
 */
void TemporaryFile::write(const std::string& content) const {
  std::ofstream ofs;
  ofs.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  try {
    ofs.open(m_filepath);
    ofs << content;
    ofs.close();
  }
  catch(std::ofstream::failure &e) {
    throw std::runtime_error("Exception while accessing temporary file : " + std::strerror(errno));
  }
}

friend std::ostream& TemporaryFile::operator<<(std::ostream& out, const TemporaryFile& temp_file) {
  out << temp_file.m_filepath;
  return out;
}

}
}
