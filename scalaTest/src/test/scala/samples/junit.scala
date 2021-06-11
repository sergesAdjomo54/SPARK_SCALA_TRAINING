package samples

import org.junit.Assert._
import org.junit._
import co.okulab.trainingTest.TestApp

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(true)

    var actual: Int = TestApp.divideVar(30,3)

    @org.junit.Test
    def test1(): Unit = {
        var expected : Int = 11
        assertEquals(expected, actual)
    }

    @Test
    def test2(): Unit = {
        var expected : Int = 10
        assertEquals(expected, actual)
    }


//    @Test
//    def testKO() = assertTrue(false)

}


