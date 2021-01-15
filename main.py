import phaseOne
import phaseTwo
import phaseThree
import phaseFour
import phaseFive


if __name__ == "__main__":
    success = phaseOne.run()
    if success == 1:
        success = phaseTwo.run()
    if success == 1:
        success = phaseThree.run()
    if success == 1:
        success = phaseFour.run()
    if success == 1:
        success = phaseFive.run()
