package org.opengauss.datachecker.common.entry.extract;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SliceExtendTest {

    private SliceExtend sliceExtendUnderTest;

    @BeforeEach
    void setUp() {
        sliceExtendUnderTest = new SliceExtend();
    }

    @Test
    void testToString() {
        SliceExtend sliceExtend = new SliceExtend();
        sliceExtend.setName("slice_name");
        sliceExtend.setNo(1);
        sliceExtend.setTableHash(10023323L);
        sliceExtend.setStartOffset(10000);
        sliceExtend.setEndOffset(20000);
        sliceExtend.setCount(10000);
        assertThat(sliceExtend).toString().equals("{ name=slice_name, no=1 , offset=[10000, 20000] , count=10000 , tableHash=10023323}");
    }
}
