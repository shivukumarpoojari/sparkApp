package com.spark.core
import org.apache.spark.sql.SparkSession
object HelloSpark {
  def main(args: Array[String]): Unit = {

    val lst1 = List((11,10000),(11,20000),(12,30000),(12,40000),(13,5000))
    val lst2 = List((11,"Hyd"),(12,"Del"),(13,"Hyd"))

    val spark = SparkSession.builder().appName("RDD Examples").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(lst1)
    val rdd2 = sc.parallelize(lst2)

    val joinRdd = rdd1.join(rdd2)
    joinRdd.collect().foreach(println)

    val citySal = joinRdd.map{x =>
      val city = x._2._2
      val sal = x._2._1
      (city,sal)
    }
    citySal.collect().foreach(println)

    //Calculate total salary from each city
    val total_salary = citySal.reduceByKey((x,y)=>(x+y))
    total_salary.collect().foreach(println)

    val emp_lst=List((11,30000,10000),(11,40000,20000),(12,50000,30000),(13,60000,20000),(12,80000,30000),(12,50000,30000))
    val emp_rdd=sc.parallelize(emp_lst)
    val emp_map=emp_rdd.map{x=>
      val id=x._1
      val dept_id=x._2
      val sal=x._3
      (id,(dept_id,sal))

    }
    val joinRdd2WithEmp=emp_map.join(rdd2)
    joinRdd2WithEmp.collect.foreach(println)
    rdd2.collect.foreach(println)

    val emp=sc.textFile("Data/emp.txt")
    val dept=sc.textFile("Data/dept.txt")
    emp.collect.foreach(println)
    dept.collect().foreach(println)
    val Emp_pair=emp.map{x=>
      val words=x.split(",")
      val deptno=words(4).toInt
      val deptid=words(0)
      val name=words(1)
      val sal =words(2)
      val sex=words(3)
      val info=deptid+":"+name+":"+sal+":"+sex
      (deptno,info)

    }

    Emp_pair.collect().foreach(println)
    val Dept_pair=dept.map{x=>
      val words=x.split(",")
      val dno=words(0).toInt
      val dname=words(1)
      val city=words(2)
      val info =words(0)+":"+words(1)+":"+words(2)
      (dno,info)

    }
    Dept_pair.collect.foreach(println)
    val joinEmpDept=Emp_pair.join(Dept_pair)
    joinEmpDept.collect().foreach(println)
    val maponjoin=joinEmpDept.map{x=>
      val Emp_info=x._2._1
      val Dept_info=x._2._2
      val info=Emp_info+","+Dept_info
      info
    }
    //maponjoin.saveAsTextFile("Data/output")




  }
}

//joinRdd
//(12,(30000,Del))
//(12,(40000,Del))
//(13,(5000,Hyd))
//(11,(10000,Hyd))
//(11,(20000,Hyd))


//(Del,30000)
//(Del,40000)
//(Hyd,5000)
//(Hyd,10000)
//(Hyd,20000)
