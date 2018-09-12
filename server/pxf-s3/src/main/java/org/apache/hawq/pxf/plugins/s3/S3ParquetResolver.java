package org.apache.hawq.pxf.plugins.s3;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

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
