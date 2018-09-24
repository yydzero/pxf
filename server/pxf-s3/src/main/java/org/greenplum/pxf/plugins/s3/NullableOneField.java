package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.OneField;

public class NullableOneField extends OneField {

	public NullableOneField(int type, Object val) {
		super(type, val);
	}

	public NullableOneField() {
		super();
	}

	/**
	 * The purpose of this class is to handle the case where <code>val</code> is
	 * null so, rather than calling <code>toString()</code> on null, it will simply
	 * return null.
	 */
	public String toString() {
		return (this.val != null) ? this.val.toString() : null;
	}

}
