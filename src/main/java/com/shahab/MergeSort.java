package com.shahab;

import java.util.ArrayList;

public class MergeSort {
    private final ArrayList<Integer> inputArray;

    public ArrayList<Integer> getSortedArray() {
        return inputArray;
    }

    public MergeSort(ArrayList<Integer> inputArray){
        this.inputArray = inputArray;
    }

    public void sortGivenArray(){
        divide(0, this.inputArray.size()-1);
    }

    public void divide(int startIndex,int endIndex){
        if(startIndex<endIndex && (endIndex-startIndex)>=1){
            int mid = (endIndex + startIndex)/2;
            divide(startIndex, mid);
            divide(mid+1, endIndex);
            merger(startIndex,mid,endIndex);
        }
    }

    public void merger(int startIndex,int midIndex,int endIndex){
        ArrayList<Integer> mergedSortedArray = new ArrayList<>();

        int leftIndex = startIndex;
        int rightIndex = midIndex+1;

        while(leftIndex<=midIndex && rightIndex<=endIndex){
            if(inputArray.get(leftIndex)<=inputArray.get(rightIndex)){
                mergedSortedArray.add(inputArray.get(leftIndex));
                leftIndex++;
            }else{
                mergedSortedArray.add(inputArray.get(rightIndex));
                rightIndex++;
            }
        }

        while(leftIndex<=midIndex){
            mergedSortedArray.add(inputArray.get(leftIndex));
            leftIndex++;
        }

        while(rightIndex<=endIndex){
            mergedSortedArray.add(inputArray.get(rightIndex));
            rightIndex++;
        }

        int i = 0;
        int j = startIndex;

        while(i<mergedSortedArray.size()){
            inputArray.set(j, mergedSortedArray.get(i++));
            j++;
        }
    }
}
