package samples

import org.scalatest._

object ScalaTestingStyles

class CalculatorSuite extends FunSuite {
  val calculator = new Calculator

  test("multiplication by 0 should always be 0"){
    assert(calculator.multiply(10, 0) == 0)
    assert(calculator.multiply(-10, 0) == 0)
    assert(calculator.multiply(0, 0) == 0)

  }

  test("division by 0 should throw some math error"){
    assertThrows[ArithmeticException](calculator.divide(10, 0))
  }
}



class CalculatorSuite1 extends FeatureSpec with GivenWhenThen {
  val calculator = new Calculator

  feature("Multiplication"){

    scenario("multiplication by 0 should always be 0"){
      Given("when the given number is 0")
      val t = 0

      When("Apply multiplication by the given number")
      val result = calculator.multiply(10, t)

      Then("The result must be zero")
      assert(result == 0)

    }
  }

  feature("Division"){
    scenario("division by 0 should throw some math error"){
      assertThrows[ArithmeticException](calculator.divide(10, 0))
    }
  }

}

















class CalculatorSpec extends FunSpec{

  val calculator = new Calculator

  describe("multiplication"){
    it("should give back 0 if multiplying by 0"){
      assert(calculator.multiply(10, 0) == 0)
      assert(calculator.multiply(-10, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }

  }


  describe("some other test"){
    it("should throw a match error if dividing by 0"){
      assertThrows[ArithmeticException](calculator.divide(10, 0))
    }
  }
}

class CalculatorWordSpec extends WordSpec {

  val calculator = new Calculator

  "A calculator" should {
    "give back 0 if multiplying by 0" in {

      assert(calculator.multiply(10, 0) == 0)
      assert(calculator.multiply(-10, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }

    "throw a math error if dividing by 0" in {
      assertThrows[ArithmeticException](calculator.divide(10, 0))
    }
  }
}


class CalculatorFreeSpec extends FreeSpec{
  val calculator = new Calculator

  "A calculator" - { //Anything you want
    "give back 0 if multiplying by 0" in {

      assert(calculator.multiply(10, 0) == 0)
      assert(calculator.multiply(-10, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }

    "throw a math error if dividing by 0" in {
      assertThrows[ArithmeticException](calculator.divide(10, 0))
    }
  }
}

//
class CalculatorPropSpec extends PropSpec{
  val calculator = new Calculator

  val multiplyByZeroExamples = List((10, 0), (-10, 0), (0, 0))

  property("Calculator multiply by 0 should be 0"){
    assert(multiplyByZeroExamples.forall{
      case (a, b) => calculator.multiply(a, b) == 0
    })
  }
}


class Calculator {
  def add(a: Int, b: Int): Int = a + b
  def subtract(a: Int, b: Int): Int = a - b
  def multiply(a: Int, b: Int): Int = a * b
  def divide(a: Int, b: Int): Int = a / b

}
