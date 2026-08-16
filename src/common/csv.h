#pragma once

#include <string>
#include <vector>

namespace rmdb_csv {

inline void StripCr(std::string& line) {
    if (!line.empty() && line.back() == '\r')
        line.pop_back();
}

// RFC 4180 fields without quoted newlines. Pointers remain valid until line changes.
inline void SplitLineInPlace(std::string& line, std::vector<const char*>& fields) {
    fields.clear();
    line.push_back('\0');
    char* write = line.data();
    const char* read = line.data();
    const char* end = line.data() + line.size() - 1;
    while (true) {
        char* field_begin = write;
        if (read < end && *read == '"') {
            ++read;
            while (read < end) {
                if (*read != '"') {
                    *write++ = *read++;
                } else if (read + 1 < end && read[1] == '"') {
                    *write++ = '"';
                    read += 2;
                } else {
                    ++read;
                    break;
                }
            }
            while (read < end && *read != ',')
                ++read;
        } else {
            while (read < end && *read != ',')
                *write++ = *read++;
        }
        *write++ = '\0';
        fields.push_back(field_begin);
        if (read >= end)
            break;
        ++read;
    }
    line.pop_back();
}

} // namespace rmdb_csv
