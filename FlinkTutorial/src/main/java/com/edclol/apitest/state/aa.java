package com.edclol.apitest.state;
/**
 * Created by Jesse on 2020/12/10 10:53
 */

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

/**
 * JessePinkMan
 */
public class aa extends GenericWriteAheadSink {


    public aa(CheckpointCommitter committer, TypeSerializer serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }

    @Override
    protected boolean sendValues(Iterable values, long checkpointId, long timestamp) throws Exception {
        return false;
    }
}
