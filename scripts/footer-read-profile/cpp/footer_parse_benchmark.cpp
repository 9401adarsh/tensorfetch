#include <arrow/io/file.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
//#include "generated/parquet_types.h"

#include <parquet/thrift/parquet_types.h>
#include <chrono>
#include <iostream>
#include <filesystem>
#include <vector>

void process_row_group_offsets(const std::shared_ptr<arrow::io::ReadableFile> &infile, const int64_t file_size,  const parquet::format::FileMetaData &thrift_meta){
    // === NEW: Prefetch row groups ===
for (const auto& rg : thrift_meta.row_groups) {
    if (rg.columns.empty()) continue;

    // Column 0 is enough to get the byte range start of the row group.
    const auto& col0 = rg.columns[0];

    int64_t rg_start = col0.file_offset; // byte offset in the parquet file
    int64_t rg_length = 0;

    // Compute total compressed size for the row group
    for (const auto& col : rg.columns) {
        rg_length += col.meta_data.total_compressed_size;
    }

    // Safety check: ensure we don't read beyond file
    if (rg_start + rg_length > file_size) {
        rg_length = file_size - rg_start;
    }

    // Allocate a temporary buffer (not used; you want the OS page cache effect)
    auto rg_buf = arrow::AllocateBuffer(rg_length).ValueOrDie();

    // Read-at = prefetch design; in future replace with io_uring SQE
    PARQUET_THROW_NOT_OK(
        infile->ReadAt(rg_start, rg_length, rg_buf->mutable_data())
    );

    // (Optional) Debug
    std::cout << "[Prefetched] RG @ " << rg_start 
              << " len=" << rg_length << " bytes\n";
    }
}

double time_selective_footer_parse_wo_fm(const std::string &file_path, std::vector<double> &times)
{
    auto mem = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocolT<apache::thrift::transport::TMemoryBuffer> proto(mem);
    auto start = std::chrono::high_resolution_clock::now();
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_path));
    int64_t file_size = std::filesystem::file_size(file_path);
    uint8_t footer_buffer[8];
    PARQUET_THROW_NOT_OK(infile->ReadAt(file_size - 8, 8, footer_buffer));
    uint32_t footer_len =
        static_cast<uint32_t>(footer_buffer[0]) |
        (static_cast<uint32_t>(footer_buffer[1]) << 8) |
        (static_cast<uint32_t>(footer_buffer[2]) << 16) |
        (static_cast<uint32_t>(footer_buffer[3]) << 24);
    if (std::memcmp(footer_buffer + 4, "PAR1", 4) != 0)
    {
        throw std::runtime_error("Invalid Parquet file (missing PAR1 magic)");
    }
    int64_t footer_start = file_size - footer_len - 8;
    auto footer_buf_arrow = arrow::AllocateBuffer(footer_len).ValueOrDie();
    PARQUET_THROW_NOT_OK(
        infile->ReadAt(footer_start, footer_len, footer_buf_arrow->mutable_data()));
    // Step 5: minimally parse row group offsets (Thrift decode)
    mem->resetBuffer(const_cast<uint8_t *>(footer_buf_arrow->data()), footer_len);
    // Clear previously parsed data
    parquet::format::FileMetaData thrift_meta; // default-construct resets it
    thrift_meta.read(&proto);
    // Access row-group offsets
    for (auto &rg : thrift_meta.row_groups)
    {
        if (!rg.columns.empty()){}
            std::cout << "RowGroup offset: " << rg.columns[0].file_offset << "\n";
        process_row_group_offsets(infile, file_size, thrift_meta);
    }
    auto end_sel = std::chrono::high_resolution_clock::now();
    double sel_ms =
        std::chrono::duration<double, std::milli>(end_sel - start).count();
    times.push_back(sel_ms);
    return sel_ms;
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        std::cerr << "Usage: ./footer_bench <file.parquet> <runs>" << std::endl;
        return 1;
    }

    std::string file_path = argv[1];
    int runs = std::stoi(argv[2]);
    std::vector<double> selective_parse_times;
    selective_parse_times.reserve(runs);
    // === Selective Parse (footer only) ===
    for(int i = 0; i < runs; i++){
        selective_parse_times.push_back(time_selective_footer_parse_wo_fm(file_path, selective_parse_times));
    }
    // === Full Parse (footer + column metadata + schema) ===
    // auto start_full = std::chrono::high_resolution_clock::now();
    // std::unique_ptr<parquet::ParquetFileReader> reader =
    //     parquet::ParquetFileReader::Open(infile);
    // std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    // int total_row_groups = metadata->num_row_groups();
    // for (int i = 0; i < total_row_groups; i++) {
    //     std::shared_ptr<parquet::RowGroupMetaData> row_group = metadata->RowGroup(i);
    //     for (int j = 0; j < row_group->num_columns(); j++) {
    //         std::shared_ptr<parquet::ColumnChunkMetaData> col_meta =
    //             row_group->ColumnChunk(j);
    //         volatile auto comp_size = col_meta->total_compressed_size();
    //     }
    // }

    // auto end_full = std::chrono::high_resolution_clock::now();
    // double full_ms =
    //     std::chrono::duration<double, std::milli>(end_full - start_full).count();
    // full_parse_times.push_back(full_ms);

    auto average = [](const std::vector<double>& v) {
        double sum = 0.0;
        for (double x : v) sum += x;
        return v.empty() ? 0.0 : sum / v.size();
    };

    double avg_sel = average(selective_parse_times);
    // std::cout << "File: " << file_path << "\n";
    // std::cout << "Runs: " << runs << "\n";
    // std::cout << "------------------------------------\n";
    std::cout << avg_sel << "\n";
    return 0;
}