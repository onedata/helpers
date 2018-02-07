/*
 * odataHelper.h
 *
 *  Created on: 5. 2. 2018
 *      Author: Jakub Valenta
 */

#ifndef HELPERS_ODATAHELPER_H_
#define HELPERS_ODATAHELPER_H_

#include "helpers/storageHelper.h"

namespace OData {

class Connection;
class DataHub;
class FileSystemNode;
}

namespace one {
namespace helpers {

class ODataFile : public FileHandle {
public:
    ODataFile(folly::fbstring fileId, std::shared_ptr<folly::Executor> executor,
        std::shared_ptr<OData::DataHub> data_hub);
    virtual ~ODataFile() = default;
    folly::Future<folly::IOBufQueue> read(
        const off_t offset, const std::size_t size) override;

    folly::Future<std::size_t> write(
        const off_t offset, folly::IOBufQueue buf) override;
    const Timeout &timeout() override;

private:
    std::shared_ptr<folly::Executor> m_executor;
    std::shared_ptr<OData::DataHub> m_data_hub;
    Timeout m_timeout;
};

class ODataHelper : public StorageHelper {
public:
    explicit ODataHelper(std::shared_ptr<folly::Executor> executor,
        const folly::fbstring &data_hub_url, const folly::fbstring &username,
        const folly::fbstring &password, const folly::fbstring &missions,
        const folly::fbstring &db_path, const folly::fbstring &tmp_path,
        const std::uint32_t tmp_size);
    virtual ~ODataHelper() = default;
    folly::Future<struct stat> getattr(const folly::fbstring &fileId) override;
    folly::Future<folly::Unit> access(
        const folly::fbstring &fileId, const int mask) override;
    folly::Future<folly::fbvector<folly::fbstring>> readdir(
        const folly::fbstring &fileId, const off_t offset,
        const std::size_t count) override;
    folly::Future<FileHandlePtr> open(const folly::fbstring &fileId,
        const int flags, const Params &openParams) override;
    folly::Future<folly::fbstring> getxattr(
        const folly::fbstring &uuid, const folly::fbstring &name) override;

    const Timeout &timeout() override;

private:
    std::shared_ptr<folly::Executor> m_executor;
    Timeout m_timeout;
    std::shared_ptr<OData::Connection> m_connection;
    std::shared_ptr<OData::DataHub> m_data_hub;
};

class ODataHelperFactory : public StorageHelperFactory {
public:
    ODataHelperFactory(asio::io_service &service);
    virtual ~ODataHelperFactory() = default;
    std::shared_ptr<StorageHelper> createStorageHelper(
        const Params &parameters) override;

private:
    asio::io_service &m_service;
};

} /* namespace helpers */
} /* namespace one */

#endif /* HELPERS_ODATAHELPER_H_ */
