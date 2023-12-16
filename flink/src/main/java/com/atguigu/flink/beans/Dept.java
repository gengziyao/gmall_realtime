package com.atguigu.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    public Integer deptno;
    public String dname;
    public Long ts;
}
