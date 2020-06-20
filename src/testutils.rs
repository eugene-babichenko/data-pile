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
