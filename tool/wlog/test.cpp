#include <cstdio>
#include <string>
#include "util.hpp"

int main()
{
  std::string st(walb::util::formatString("%s%s", 
      "0123456789", "0123456789"));
  printf("%s %zu\n", st.c_str(), st.size());

  std::string st2(walb::util::formatString(""));
  printf("%s %zu\n", st2.c_str(), st2.size());
}
