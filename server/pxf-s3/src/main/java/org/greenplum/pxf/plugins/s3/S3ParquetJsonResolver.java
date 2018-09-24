package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadResolver;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.utilities.Plugin;

import java.util.ArrayList;
import java.util.List;

public class S3ParquetJsonResolver extends Plugin implements ReadResolver {

	public S3ParquetJsonResolver(InputData input) {
		super(input);
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		List<OneField> record = new ArrayList<>();
		record.add(new NullableOneField(DataType.VARCHAR.getOID(), row.getData()));
		return record;
	}

}
