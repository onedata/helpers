/**
 * @file storageHelperFactory.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.h"

#include "clusterProxyHelper.h"
#include "communication/communicator.h"
#include "directIOHelper.h"

#include <boost/algorithm/string/case_conv.hpp>

namespace one
{
namespace helpers
{

BufferLimits::BufferLimits(const size_t wgl, const size_t rgl, const size_t wfl,
                           const size_t rfl, const size_t pbs)
    : writeBufferGlobalSizeLimit{wgl}
    , readBufferGlobalSizeLimit{rgl}
    , writeBufferPerFileSizeLimit{wfl}
    , readBufferPerFileSizeLimit{rfl}
    , preferedBlockSize{pbs}
{
}

namespace utils {

    std::string tolower(std::string input) {
        boost::algorithm::to_lower(input);
        return input;
    }

} // namespace utils

StorageHelperFactory::StorageHelperFactory(std::shared_ptr<communication::Communicator> communicator,
                                           const BufferLimits &limits, boost::asio::io_service &dio_service,
                                           boost::asio::io_service &cproxy_service)
    : m_communicator{std::move(communicator)}
    , m_limits{limits}
    , m_dio_service{dio_service}
    , m_cproxy_service{cproxy_service}
{
}

std::shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(const std::string &sh_name,
                                                                       const IStorageHelper::ArgsMap &args) {
    if(sh_name == "DirectIO")
        return std::make_shared<DirectIOHelper>(args, m_dio_service);

    if(sh_name == "ClusterProxy")
        return std::make_shared<ClusterProxyHelper>(m_communicator, m_limits, args, m_cproxy_service);

    return {};
}

std::string srvArg(const int argno)
{
    return "srv_arg" + std::to_string(argno);
}

} // namespace helpers
} // namespace one
