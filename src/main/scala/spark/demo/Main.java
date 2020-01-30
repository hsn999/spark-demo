package spark.demo;

import spark.demo.*;


public class Main {
    public static void main(String[] args) {
        /*// 调用scala中的方法，方法为object下的成员，都是静态方法，java中可以直接调用
        String info = ScalaMain.sayScala(1);
        System.out.println(info);
 
        // 创建class修饰的ScalaMain对象，在其内部的方法都是一些实例方法，通过对象访问
        ScalaMain sm = new ScalaMain(); 
        String normal = sm.normal(100);
        System.out.println(normal);
        
        ScalaTestClass st = new ScalaTestClass();
        st.hix("dd");
        
        KMeansExample.main(null);*/
    }
 
    /**
     * 静态方法
     * @return
     */
    public static int sayJava() {
        System.out.println("This is Java static method");
        return 1;
    }
 
    /**
     * 普通方法
     * @param age
     * @return
     */
    public int sayJava(int age){
        System.out.println("This is Java normal method");
        return age;
    }
}
