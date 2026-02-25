package tech.ydb.jooq;

import java.util.List;

import jooq.generated.ydb.default_schema.tables.records.SeriesRecord;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.Test;

import static jooq.generated.ydb.default_schema.Tables.SERIES;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchReplaceTest extends BaseTest {

    @Test
    public void testSimpleInsertViaBatchUpsert() {
        SeriesRecord newRecord = new SeriesRecord();
        newRecord.setSeriesId(ULong.valueOf(1));
        newRecord.setTitle("New Series");
        newRecord.setSeriesInfo("Info about the new series");
        newRecord.setReleaseDate(ULong.valueOf(20220101));

        dsl.batchReplace(newRecord)
                .execute();

        Result<SeriesRecord> upsertedRecord = dsl.selectFrom(SERIES)
                .where(SERIES.SERIES_ID.eq(ULong.valueOf(1)))
                .fetch();

        assertEquals(List.of(newRecord), upsertedRecord);
    }

    @Test
    public void testMultipleInsertViaBatchUpsert() {
        List<SeriesRecord> records = getExampleRecords();

        dsl.batchReplace(records)
                .execute();

        Result<SeriesRecord> upsertedRecords = dsl.selectFrom(SERIES).fetch();

        assertEquals(records, upsertedRecords);
    }

}
