package samples

import co.okulab.trainingTest.TestApp
import org.scalatest.FunSpec


class Scala_Test extends FunSpec
{

  it("should match"){

    assert(0 == TestApp.divideVar(30,3))
  }

  it("should match2"){

    assert(10 == TestApp.divideVar(30,3))
  }

}
