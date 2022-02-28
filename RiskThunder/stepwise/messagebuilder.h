

#ifndef STEPWISE_MESSAGE_BUILDER_H
#define STEPWISE_MESSAGE_BUILDER_H

#include <sstream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>

#include "basetype.h"
#include "network-message.h"
#include "traverse-message.h"

//namespace elabs{

    class MessageBuilder{
    private:
        PropertyStringMap  cfgs_;
    public:
        static MessageBuilder& instance(){
            static MessageBuilder builder;
            return builder;
        }

        int initialize(const std::string& app_id, const std::string& secret_key );
        static ByteArray marshall(const std::shared_ptr<TraverseMessage> & message);
        static TraverseMessage::Ptr unmarshall( const ByteArray&  data );


    };

//}


#endif //STEPWISE_NETWORK_MESSAGE_H
