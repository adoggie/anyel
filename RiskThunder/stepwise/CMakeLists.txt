cmake_minimum_required(VERSION 3.18)
project(stepwise)

set(CMAKE_CXX_STANDARD 11)
link_directories( /usr/local/lib   )
include_directories(/usr/local/include)

link_libraries(
        pthread jsoncpp
         )

add_executable(stepwise main.cpp network-message.h network-message.cpp
        traverse-message.cpp traverse-message.h
        basetype.h base64.h base64.cpp
        messagebuilder.h messagebuilder.cpp
        )


# cmake .. "-DCMAKE_OSX_ARCHITECTURES=arm64"  编译macos版本