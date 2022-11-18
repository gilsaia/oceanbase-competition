#pragma once

#include "lib/file/ob_file.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include <atomic>

namespace oceanbase
{
namespace sql
{

class ObLoadDataBuffer
{
public:
  ObLoadDataBuffer();
  ~ObLoadDataBuffer();
  void reuse();
  void reset();
  int create(int64_t capacity);
  int squash();
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE char *end() const { return data_ + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
  OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
  OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }
private:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t capacity_;
};

class ObLoadSequentialFileReader
{
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  int read_next_buffer(ObLoadDataBuffer &buffer);
private:
  common::ObFileReader file_reader_;
  int64_t offset_;
  bool is_read_end_;
};

class ObLoadCSVPaser
{
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadDataBuffer &buffer, common::ObNewRow *&row);
private:
  struct UnusedRowHandler
  {
    int operator()(common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line)
    {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    }
  };
private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  ObCSVGeneralParser csv_parser_;
  common::ObNewRow row_;
  UnusedRowHandler unused_row_handler_;
  common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
  bool is_inited_;
};

class ObLoadDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObLoadDatumRow();
  ~ObLoadDatumRow();
  void reset();
  int init(int64_t capacity);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos);
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
  DECLARE_TO_STRING;
public:
  common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

class ObLoadDatumRowCompare
{
public:
  ObLoadDatumRowCompare();
  ~ObLoadDatumRowCompare();
  int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
  bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  int result_code_;
private:
  int64_t rowkey_column_num_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  bool is_inited_;
};

class ObLoadRowCaster
{
public:
  ObLoadRowCaster();
  ~ObLoadRowCaster();
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row, const ObLoadDatumRow *&datum_row);
private:
  int init_column_schemas_and_idxs(
    const share::schema::ObTableSchema *table_schema,
    const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                        const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  ObLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
  bool is_inited_;
};

class ObLoadExternalSort
{
public:
  ObLoadExternalSort();
  ~ObLoadExternalSort();
  int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
           int64_t file_buf_size);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
  void set_finish(bool flag) { is_finish_ = flag; }
  int get_next_row(const ObLoadDatumRow *&datum_row);
  int transfer_final_sorted(ObLoadExternalSort &merge_sorter);
  bool finish();
private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
public:
  storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
private:
  bool is_closed_;
  bool is_inited_;
  std::atomic<bool> is_finish_{true};
  
};

class ObLoadSSTableWriter
{
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  int init(const share::schema::ObTableSchema *table_schema);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
private:
  int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);
  int init_macro_block_writer(const share::schema::ObTableSchema *table_schema);
  int create_sstable();
private:
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
  blocksstable::ObDatumRow datum_row_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadDataDirectDemo : public ObLoadDataBase
{
  static const int64_t MEM_BUFFER_SIZE = (1LL << 30); // 1G
  static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
  static const uint32_t PARALLEL_LOAD_NUM = 8;
public:
  ObLoadDataDirectDemo();
  virtual ~ObLoadDataDirectDemo();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
private:
  int inner_init(ObLoadDataStmt &load_stmt);
  int parallel_row_caster_init(ObLoadDataStmt &load_stmt, const ObTableSchema *table_schema);
  int parallel_external_sort_init(const ObTableSchema *table_schema);
  int combine_sort();
  int do_load();
private:
  struct DoOneRowTask {
    int idx;
    const ObNewRow *new_row;
  };
  class : public ObSimpleThreadPool {
    void handle(void *task) {
      DoOneRowTask cur_task = *(DoOneRowTask *)task;
      int idx = cur_task.idx;
      const ObNewRow *new_row = cur_task.new_row;
      int ret = OB_SUCCESS;
      const ObLoadDatumRow *datum_row = nullptr;
      if (OB_FAIL(row_casters_[idx].get_casted_row(*new_row, datum_row))) {
        LOG_WARN("fail to cast row", KR(ret));
      } else if (OB_FAIL(external_sorts_[idx].append_row(*datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
      delete new_row;
      external_sorts_[idx].set_finish(true);
    }
  public:
    int inner_init(const int64_t thread_num, const int64_t task_num_limit, 
                    ObLoadRowCaster* row_casters, ObLoadExternalSort* external_sorts) {
      row_casters_ = row_casters;
      external_sorts_ = external_sorts; 
      int ret = OB_SUCCESS;
      init(thread_num, task_num_limit);
      return ret;
    }
    ObLoadRowCaster* row_casters_;
    ObLoadExternalSort* external_sorts_;
  } pool_;
  ObLoadCSVPaser csv_parser_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
  //ObLoadRowCaster row_caster_;
  ObLoadRowCaster parallel_row_caster_[PARALLEL_LOAD_NUM];
  //ObLoadExternalSort external_sort_;
  ObLoadExternalSort parallel_external_sort_[PARALLEL_LOAD_NUM];
  ObLoadExternalSort combine_external_sort_;
  DoOneRowTask task[PARALLEL_LOAD_NUM];
  ObLoadSSTableWriter sstable_writer_;
};

} // namespace sql
} // namespace oceanbase
