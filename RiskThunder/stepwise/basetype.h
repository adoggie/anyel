//
// Created by scott on 2022/2/20.
//

#ifndef STEPWISE_BASETYPE_H
#define STEPWISE_BASETYPE_H

#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>

//namespace elabs{

//    namespace thunder{
        #define DIRECTION_BUY   "buy"
        #define DIRECTION_SELL  "sell"
        #define OC_OPEN  "open"
        #define OC_CLOSE  "close"
        #define OC_CLOSET  "closeT"
        #define OC_CLOSEY  "closeY"

        #define MESSAGE_SEND_ORDER_REQ    "SEND_ORDER_REQ"
        #define MESSAGE_SEND_ORDER_RESP   "SEND_ORDER_RESP"
        #define MESSAGE_ON_TRADE   "ON_TRADE"
        #define MESSAGE_THUNDER_STATUS   "THUNDER_STATUS"
        #define MESSAGE_INFO_HY   "INFO_HY"
        #define MESSAGE_INFO_ORDER   "INFO_ORDER"



//    }
//}

//namespace elabs {
    typedef unsigned char Byte;
    typedef std::vector <Byte> ByteArray;
    typedef std::map <std::string, std::string> PropertyStringMap;

//}
#endif //STEPWISE_BASETYPE_H
