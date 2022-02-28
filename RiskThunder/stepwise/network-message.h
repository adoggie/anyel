

#ifndef STEPWISE_NETWORK_MESSAGE_H
#define STEPWISE_NETWORK_MESSAGE_H

#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>

#include "basetype.h"

//namespace elabs{

    struct TraverseMessage;
    struct NetworkMessage{
        typedef std::shared_ptr< NetworkMessage > Ptr;

        std::string ver;           
        std::string dest_service;   
        std::string dest_id;      
        std::string msg_type;     
        std::string from_service;  
        std::string from_id;        
        long long  timestamp;    
        std::string signature;     
        std::string encode;        

        NetworkMessage();
        ByteArray marshall(const std::shared_ptr<TraverseMessage>& payload,const std::string& encode="base64") ;
    };

//}


#endif //STEPWISE_NETWORK_MESSAGE_H
