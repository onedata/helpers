/*
 * odataHelper.cpp
 *
 *  Created on: 5. 2. 2018
 *      Author: Jakub Valenta
 */

#include "odataHelper.h"

#include "asioExecutor.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>
#include <folly/FBString.h>
#include <folly/io/IOBufQueue.h>
#include <memory>
#include <odata/DataHub.h>
#include <odata/DataHubConnection.h>
#include <odata/FileSystemNode.h>
#include <unistd.h>

namespace one {
namespace helpers {

namespace {
std::vector<std::string> parseMissions(const std::string &missions)
{
    std::vector<std::string> splited;
    boost::split(splited, missions, [](char c) { return c == ','; });
    return splited;
}
}

ODataFile::ODataFile(folly::fbstring fileId,
    std::shared_ptr<folly::Executor> executor,
    std::shared_ptr<OData::DataHub> data_hub)
    : FileHandle(std::move(fileId))
    , m_executor(std::move(executor))
    , m_data_hub(std::move(data_hub))
    , m_timeout(ASYNC_OPS_TIMEOUT)
{
}

folly::Future<folly::IOBufQueue> ODataFile::read(
    const off_t offset, const std::size_t size)
{
    return folly::via(m_executor.get(), [=]() {
        folly::IOBufQueue buffer;
        const auto content = m_data_hub->getFile(
            boost::filesystem::path(m_fileId.c_str()), offset, size);
        buffer.append(content.data(), content.size());
        return folly::makeFuture(std::move(buffer));
    });
}

folly::Future<std::size_t> ODataFile::write(
    const off_t offset, folly::IOBufQueue buf)
{
    return folly::via(m_executor.get(), []() {
        return folly::makeFuture<std::size_t>(std::system_error{
            std::make_error_code(std::errc::function_not_supported)});
    });
}

const Timeout &ODataFile::timeout() { return m_timeout; }

ODataHelper::ODataHelper(std::shared_ptr<folly::Executor> executor,
    const folly::fbstring &data_hub_url, const folly::fbstring &username,
    const folly::fbstring &password, const folly::fbstring &missions,
    const folly::fbstring &db_path, const folly::fbstring &tmp_path,
    const std::uint32_t tmp_size)
    : m_executor(std::move(executor))
    , m_timeout(ASYNC_OPS_TIMEOUT)
    , m_connection(
          std::make_shared<OData::DataHubConnection>(data_hub_url.toStdString(),
              username.toStdString(), password.toStdString()))
    , m_data_hub(std::make_shared<OData::DataHub>(*m_connection,
          parseMissions(missions.toStdString()), db_path.toStdString(),
          tmp_path.toStdString(), tmp_size))
{
}

folly::Future<struct stat> ODataHelper::getattr(const folly::fbstring &fileId)
{
    return folly::via(m_executor.get(), [=]() {
        const auto file = m_data_hub->getFile(fileId.toStdString());
        if (file == nullptr) {
            return folly::makeFuture<struct stat>(std::system_error{
                std::make_error_code(std::errc::no_such_file_or_directory)});
        }
        struct stat attr;
        std::memset(&attr, 0, sizeof(attr));
        attr.st_uid = getuid();
        attr.st_gid = getgid();
        if (file->isDirectory()) {
            attr.st_mode = 0555;
        }
        else {
            attr.st_mode = 0444;
        }
        attr.st_size = file->getSize();

        return folly::makeFuture<struct stat>(std::move(attr));
    });
}

folly::Future<folly::Unit> ODataHelper::access(
    const folly::fbstring &fileId, const int mask)
{
    return folly::via(m_executor.get(), [=]() {
        const auto file = m_data_hub->getFile(fileId.toStdString());
        if (file == nullptr) {
            return folly::makeFuture<folly::Unit>(std::system_error{
                std::make_error_code(std::errc::no_such_file_or_directory)});
        }
        return folly::makeFuture();
    });
}

folly::Future<folly::fbvector<folly::fbstring>> ODataHelper::readdir(
    const folly::fbstring &fileId, const off_t offset, const std::size_t count)
{
    return folly::via(m_executor.get(), [=]() {
        const auto directory = m_data_hub->getFile(fileId.toStdString());
        if (directory == nullptr) {
            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::system_error{std::make_error_code(
                    std::errc::no_such_file_or_directory)});
        }
        else if (directory->isDirectory()) {
            auto dir_content = directory->readDir();
            dir_content.push_back(".");
            dir_content.push_back("..");
            folly::fbvector<folly::fbstring> dir;
            for (auto i = static_cast<std::size_t>(offset);
                 i < dir_content.size() && i < offset + count; ++i) {
                dir.push_back(folly::fbstring(dir_content[i]));
            }
            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::move(dir));
        }
        else {
            return folly::makeFuture<folly::fbvector<folly::fbstring>>(
                std::system_error{
                    std::make_error_code(std::errc::not_a_directory)});
        }
    });
}

folly::Future<FileHandlePtr> ODataHelper::open(
    const folly::fbstring &fileId, const int flags, const Params &openParams)
{
    return folly::via(m_executor.get(), [=]() {
        const auto file = m_data_hub->getFile(fileId.toStdString());
        if (file == nullptr) {
            return folly::makeFuture<FileHandlePtr>(std::system_error{
                std::make_error_code(std::errc::no_such_file_or_directory)});
        }
        else if (file->isDirectory()) {
            return folly::makeFuture<FileHandlePtr>(std::system_error{
                std::make_error_code(std::errc::is_a_directory)});
        }
        return folly::makeFuture<FileHandlePtr>(
            std::make_shared<ODataFile>(fileId, m_executor, m_data_hub));
    });
}

folly::Future<folly::fbstring> ODataHelper::getxattr(
    const folly::fbstring &uuid, const folly::fbstring &name)
{
    return folly::via(m_executor.get(),
        []() { return makeFuturePosixException<folly::fbstring>(ENOTSUP); });
}

const Timeout &ODataHelper::timeout() { return m_timeout; }

ODataHelperFactory::ODataHelperFactory(asio::io_service &service)
    : m_service(service)
{
}

std::shared_ptr<StorageHelper> ODataHelperFactory::createStorageHelper(
    const Params &parameters)
{
    const auto &url = getParam(parameters, "datahubUrl");
    const auto &username = getParam(parameters, "username");
    const auto &password = getParam(parameters, "password");
    const auto &missions = getParam(parameters, "missions");
    const auto &db_path = getParam(parameters, "databasePath");
    const auto &tmp_path = getParam(parameters, "tmpPath");
    const auto &tmp_size = getParam(parameters, "tmpSize");
    return std::make_shared<ODataHelper>(
        std::make_shared<AsioExecutor>(m_service), url, username, password,
        missions, db_path, tmp_path, std::stoi(tmp_size.toStdString()));
}

} /* namespace helpers */
} /* namespace one */
