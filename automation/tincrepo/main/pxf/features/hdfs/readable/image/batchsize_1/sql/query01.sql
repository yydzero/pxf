-- @description query01 for PXF HDFS Readable images

SELECT image_test_batchsize_1.names FROM image_test_batchsize_1, compare_table_batchsize_1
    WHERE image_test_batchsize_1.images = compare_table_batchsize_1.images
    ORDER BY names;
