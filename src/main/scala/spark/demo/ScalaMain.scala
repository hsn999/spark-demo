package spark.demo

/**
 * 伴生对象，与类共享名字，可以访问类的私有属性和方法
 * 在object中一般可以为伴生类做一些初始化等操作
 */
object ScalaMain {

  def main(args: Array[String]): Unit = {
    //    sayScala(1)
    Main.sayJava() // 调用java静态方法
    val main = new Main()
    main.sayJava(12) // 调用java普通方法
  }

  def sayScala(a: Int): String = {
    println("this is scala method")
    "method"
  }
}

/**
 * 伴生类，其内部的方法都是实例方法
 */
class ScalaMain {

  def normal(a: Int): String = {
    println("this is a scala normal method")
    "normal"
  }


}