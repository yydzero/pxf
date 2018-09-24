package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.ReadResolver;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.utilities.Plugin;

import java.util.List;

public class S3ParquetResolver extends Plugin implements ReadResolver {

	public S3ParquetResolver(InputData input) {
		super(input);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		return (List<OneField>) row.getData();
	}

}
