import sys
import statistics

arquivo = sys.argv[1]

with open(arquivo) as f:
    valores = [float(l.strip()) for l in f if l.strip()]

if not valores:
    print("Nenhuma amostra encontrada.")
    sys.exit(1)

print(f"Arquivo: {arquivo}")
print(f"Amostras : {len(valores)}")
print(f"Média    : {statistics.mean(valores):.3f} ms")
print(f"Mediana  : {statistics.median(valores):.3f} ms")
print(f"Desvio   : {statistics.stdev(valores):.3f} ms")
print(f"Mínimo   : {min(valores):.3f} ms")
print(f"Máximo   : {max(valores):.3f} ms")
