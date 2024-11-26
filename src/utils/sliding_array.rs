use std::{
    collections::VecDeque,
    ops::{Index, IndexMut},
};

pub struct SlidingArray<T> {
    data: VecDeque<T>,
    first_index: usize,
}

impl<T> SlidingArray<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(capacity),
            first_index: 0,
        }
    }

    pub fn push_back(&mut self, value: T) -> usize {
        self.data.push_back(value);
        self.first_index + self.data.len() - 1
    }

    pub fn push_front(&mut self, value: T) {
        self.data.push_front(value);
        self.first_index -= 1;
    }

    pub fn pop_front(&mut self) -> Option<T> {
        let value = self.data.pop_front()?;
        self.first_index += 1;
        Some(value)
    }

    pub fn first_index(&self) -> usize {
        self.first_index
    }

    pub fn next_index(&self) -> usize {
        self.first_index + self.data.len()
    }

    pub fn total_size(&self) -> usize {
        self.first_index + self.data.len()
    }

    pub fn data(&self) -> &VecDeque<T> {
        &self.data
    }

    pub fn enumerate_mut(&mut self) -> impl Iterator<Item = (usize, &mut T)> {
        self.data
            .iter_mut()
            .enumerate()
            .map(|(i, v)| (i + self.first_index, v))
    }

    pub fn back(&self) -> Option<&T> {
        self.data.back()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.first_index {
            return None;
        }
        self.data.get(index - self.first_index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index < self.first_index {
            return None;
        }
        self.data.get_mut(index - self.first_index)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl<T> Index<usize> for SlidingArray<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("Index out of bounds")
    }
}

impl<T> IndexMut<usize> for SlidingArray<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("Index out of bounds")
    }
}

impl<T> Default for SlidingArray<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}
