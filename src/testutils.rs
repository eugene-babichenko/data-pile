use quickcheck::{Arbitrary, Gen};

#[derive(Debug, Clone)]
pub struct TestData {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Arbitrary for TestData {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        Self {
            key: Arbitrary::arbitrary(g),
            value: Arbitrary::arbitrary(g),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FixLenTestData {
    pub key: [u8; 32],
    pub value: Vec<u8>,
}

impl Arbitrary for FixLenTestData {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        Self {
            key: {
                let mut key = [0u8; 32];
                for i in 0..key.len() {
                    key[i] = Arbitrary::arbitrary(g);
                }
                key
            },
            value: Arbitrary::arbitrary(g),
        }
    }
}
