package com.github.fwi.taskq2.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import nl.fw.util.jdbc.DbConnUtil;

public class TaskDataSerializer {

	public byte[] taskDataToBytes(Object tdata) throws IOException {
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bout)) {
			out.writeObject((Serializable)tdata);
		}
		return bout.toByteArray();
	}

	public byte[] taskDataToBytesRe(Object tdata)  {
		
		byte[] ba = null;
		try {
			ba = taskDataToBytes(tdata);
		} catch (Exception e) {
			DbConnUtil.rethrowRuntime(e);
		}
		return ba;
	}

	public Object bytesToTaskData(byte[] ba) throws IOException, ClassNotFoundException {
		
		Object o = null;
		ByteArrayInputStream bin = new ByteArrayInputStream(ba);
		try (ObjectInputStream in = new ObjectInputStream(bin)) {
			o = in.readObject();
		}
		return o;
	}

	public Object bytesToTaskDataRe(byte[] ba)  {
		
		Object tdata = null;
		try {
			tdata = bytesToTaskData(ba);
		} catch (Exception e) {
			DbConnUtil.rethrowRuntime(e);
		}
		return tdata;
	}

}
