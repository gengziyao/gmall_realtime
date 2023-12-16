package com.atguigu.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
public Integer empno;
public String ename;
public Integer deptno;
public Long ts;
}
