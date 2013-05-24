This project verifies Goldbach's conjecture by finding two prime numbers that sum to each even number.  Right now there are two appraoches.

  - Storm/Mongo "enterprise" solution.  First 50,000,000 primes are stored in an indexed MongoDB collection and called upon to look for candidates that sum to each even number.  Storm bolts can be scaled indefinitely to provide faster computation.
  - CPP solution

Goldbach's conjecture states that "Every even integer greater than 2 can be expressed as the sum of two primes." Source: [Goldbach's Conjecture](http://en.wikipedia.org/wiki/Goldbach's_conjecture)
