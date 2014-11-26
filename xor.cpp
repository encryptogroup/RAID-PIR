#include <inttypes.h>
#include <algorithm>
#include <boost/lambda/lambda.hpp>
#include <boost/python.hpp>
/*
source: 
  http://stackoverflow.com/questions/2119761/simple-python-challenge-fastest-bitwise-xor-on-data-buffers

might compile with:
  g++ -shared -Wl,-soname,"xorcpp.so" -L/usr/local/lib xorcpp.o -lboost_python -fpic -o xorcpp.so
or
  g++  -I/usr/include/python2.7 -fpic  xorcpp.cpp -shared -lboost_python -o xorcpp.so

*/
namespace { 
  namespace py = boost::python;

  template<class InputIterator, class InputIterator2, class OutputIterator>
  void
  xor_(InputIterator first, InputIterator last, 
       InputIterator2 first2, OutputIterator result) {
    // `result` might `first` but not any of the input iterators
    namespace ll = boost::lambda;
    (void)std::transform(first, last, first2, result, ll::_1 ^ ll::_2);
  }

  template<class T>
  py::str 
  xorcpp_str_inplace(const py::str& a, py::str& b) {
    const size_t alignment = std::max(sizeof(T), 16ul);
    const size_t n         = py::len(b);
    const char* ai         = py::extract<const char*>(a);
    char* bi         = py::extract<char*>(b);
    char* end        = bi + n;

    if (n < 2*alignment) 
      xor_(bi, end, ai, bi);
    else {
      const ptrdiff_t head = (alignment - ((size_t)bi % alignment))% alignment;
      const ptrdiff_t tail = (size_t) end % alignment;
      xor_(bi, bi + head, ai, bi);
      xor_((const T*)(bi + head), (const T*)(end - tail), 
           (const T*)(ai + head),
           (T*)(bi + head));
      if (tail > 0) xor_(end - tail, end, ai + (n - tail), end - tail);
    }
    return b;
  }

}

BOOST_PYTHON_MODULE(xorcpp)
{
  py::def("xorcpp_inplace", xorcpp_str_inplace<int64_t>);     // for stringsz
}