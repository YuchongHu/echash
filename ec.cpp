#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<string.h>
#include<isa-l.h>

#define LEN 10
#define M 6
#define K 4 
 
unsigned char* creat_str(int k)
{
    unsigned char* s=(unsigned char *)malloc(LEN*sizeof(unsigned char));
    unsigned char *p=s;
    int i=0;

    srand((unsigned) time(NULL)+k);
    for(i=0;i<LEN;i++)
    {
        *p=rand() % 256;
        p++;
    }

    return s;
}

//err number should be set in order
int main(int argc,char* argv[])
{
    if(argc!=3)
    {
        printf("command was wrong!\n");
        return -1;
    }

    int err=argc-1;
    int err_arr[2]={0,0};
    sscanf(argv[1],"%d",&err_arr[0]);
    sscanf(argv[2],"%d",&err_arr[1]);
    printf("ERROR NUMBER:%d and %d\n",err_arr[0], err_arr[1]);
    
    unsigned char* source_data[M];
    int i,j,r;
    for(i=0;i<M;i++)
    {
        source_data[i]=(unsigned char*)calloc(LEN,sizeof(char*));
    }

    //Assign the source_data
    for(i=0;i<K;i++)
    {
        memcpy(source_data[i],creat_str(i),LEN);
    }
    
    unsigned char encode_gftbl[32*K*(M-K)];
    unsigned char encode_matrix[M*K]={0};

    gf_gen_cauchy1_matrix(encode_matrix, M, K);
    printf("encode_matrix:\n");
    for(i=0;i<M*K;)
    {
        printf("%3d ",encode_matrix[i]);
        i++;
        if(i%K==0)
            printf("\n");
    }

    ec_init_tables(K, M-K, &encode_matrix[K*K], encode_gftbl);
    ec_encode_data(LEN, K, M-K , encode_gftbl, source_data, &source_data[K]);

    printf("\nEncoding...\nsource_data:\n");
    for(i=0;i<M;i++)
    {
        for(j=0;j<LEN;j++)
            printf("%3d ",source_data[i][j]);
        printf("\n");
    }

    printf("\nAssume %d and %d failed.\n",err_arr[0],err_arr[1]);
    printf("\nDecoding...\nsource_data Left:\n");
    unsigned char* left_data[M-2];
    for(i=0,r=0;i<M;i++)
    {
        if(i==err_arr[0]||i==err_arr[1])
            continue;
        left_data[r]=(unsigned char*)calloc(LEN,sizeof(char*));
        memcpy(left_data[r++],source_data[i],LEN);
    }
    for(i=0;i<r;i++)
    {
        for(j=0;j<LEN;j++)
            printf("%3d ",left_data[i][j]);
        printf("\n");
    }

    printf("\nInvert_matrix:\n");

    unsigned char error_matrix[M*K]={0};
    unsigned char invert_matrix[M*K]={0};
    unsigned char decode_matrix[M*K]={0};
  
    for(i=0,r=0;i<M;i++)
    {
        if(i==err_arr[0]||i==err_arr[1])
            continue;
        for(j=0;j<K;j++)
            error_matrix[r*K+j]=encode_matrix[i*K+j];
        r++;
    }

    gf_invert_matrix(error_matrix,invert_matrix,K);
    
    for(i=0;i<M*K;)
    {
        printf("%3d ",invert_matrix[i]);
        i++;
        if(i%K==0)
            printf("\n");
    }

    //get the failed data
    int e=0;
    for(e=0;e<2;e++)
    {
        int idx=err_arr[e];
        if(idx<K)// We lost one of the buffers containing the data
        {
            for(j=0;j<K;j++)
                decode_matrix[e*K+j]=invert_matrix[idx*K+j];
        }
        else// We lost one of the buffer containing the error correction codes
        {
            for(i=0;i<K;i++)
            {
                unsigned char s=0;
                //mul the encode matrix coefficient to get the failed data only
                for(j=0;j<K;j++)
                    s^=gf_mul(invert_matrix[j*K+i],encode_matrix[idx*K+j]);
                decode_matrix[e*K+i]=s;
            }
        }
    }

    printf("\nDecode_matrix:\n");
    for(i=0;i<M*K;)
    {
        printf("%3d ",decode_matrix[i]);
        i++;
        if(i%K==0)
            printf("\n");
    }


    unsigned char decode_gftbl[32*K*(M-K)];
    unsigned char* recovery_data[2];

    for(i=0;i<2;i++)
    {
        recovery_data[i]=(unsigned char*)calloc(LEN,sizeof(char*));
    }

    ec_init_tables(K, M-K, decode_matrix, decode_gftbl);
    ec_encode_data(LEN, K, M-K , decode_gftbl, left_data, recovery_data);

    printf("Decode the recovery_data:\n");
    for(i=0;i<2;i++)
    {
        for(j=0;j<LEN;j++)
            printf("%3d ",recovery_data[i][j]);
        printf("\n");
    }

    return 0;
}