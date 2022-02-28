cmake_minimum_required(VERSION 2.8)
project(ctpticker)

set(CMAKE_CXX_STANDARD 11)

set(Boost_USE_MULTITHREADED      ON)

#option(zsqh_test "zsqh_test" OFF)
#option(zsqh_prd "zsqh_prd" ON)
#option(simnow "simnow" OFF)
#option(htqh_test "htqh_test" OFF)
#option(htqh_prd "htqh_prd" OFF)
#option(simnow ON)

#find_package(Boost 1.59.0 COMPONENTS python thread date_time system chrono REQUIRED)
link_directories( /usr/local/lib   )
#link_directories("/usr/local/lib"   )
add_definitions(-D_CHUANTOU=1)
include_directories(/usr/local/include
        #./ctpapi/huawen/6.5.1_cp
        ./ctpapi/zsqh/product/6.3.15
        /Users/admin/projects/homebrew/include
        /usr/local/include )
link_libraries(  boost_system boost_thread boost_date_time redis++ hiredis
        #thosttraderapi_se  thostmduserapi_se
        thosttraderapi_se_zsqh_prd  thostmduserapi_se_zsqh_prd
        pthread jsoncpp
        zmq )
add_executable(ctpticker
        main.cpp app.cpp config.cpp
        Controller.cpp
        error.h error.cpp
        utils/logger.cpp utils/logger.h

      #  utils/mongoose.h utils/mongoose.c

        trade.cpp trade.h
        market.cpp market.h
        behavior.h
        #utils/HttpClient.cpp utils/HttpClient.h
        PosReceiver.cpp PosReceiver.h RegistryClient.cpp RegistryClient.h
        pubservice.cpp pubservice.h mx.h mx.cpp behavior.cpp
        mx.h mx.cpp
       # HttpService.h HttpService.cpp
        )
