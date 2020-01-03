
#ifndef __COMMON_TEMPORARY_FILE_HPP__
#define __COMMON_TEMPORARY_FILE_HPP__

#include <iostream>
#include <string>

namespace mesos {
namespace internal {

/*
 * Represent a temporary file that can be either written or read from.
 */
class TemporaryFile {
 public:
  TemporaryFile();
  std::string readAll() const ;
  void write(const std::string& content) const;
  inline const std::string& filepath() const { return m_filepath; }
  friend std::ostream& operator<<(std::ostream& out, const TemporaryFile& temp_file) ;

 private:
  std::string m_filepath;
};

}
}

#endif // __COMMON_TEMPORARY_FILE_HPP__
